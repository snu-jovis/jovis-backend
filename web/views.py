import os
import time
import re

import psycopg2
from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response

from backend.settings import PG_LOG_FILE, PG_LOG_BACKUP_DIR

DEBUG = False

def clear_previous_log():
    os.system(f"cp {PG_LOG_FILE} {PG_LOG_BACKUP_DIR}/{time.time()}_prev")
    os.system(f"echo '' > {PG_LOG_FILE}")

def read_and_clear_log():
    filename = f"{PG_LOG_BACKUP_DIR}/{time.time()}_pq"

    # TODO: I have no idea why logfile from pg_ctl does not suppress 
    # STATEMENT even though I turned off log configurations.
    # So, I manually remove the statements here to save disk and parsing cost.
    f1 = open(PG_LOG_FILE, 'r')
    ret = []
    recent_removed = False
    for line in f1.readlines():
        if 'STATEMENT' in line:
            recent_removed = True
            continue

        if recent_removed and line[0] == '\t':
            continue

        recent_removed = False
        ret.append(line)

    f1.close()

    os.system(f"echo '' > {PG_LOG_FILE}")

    f2 = open(filename, 'w')
    for line in ret:
        f2.write(line)
    f2.close()

    return ret

def decide_next_state(logs, cur):
    # check indentation width
    raw_cur_line, raw_prev_line = logs[cur].replace('\t', '    '), logs[cur-1].replace('\t', '    ')
    cur_indent = len(raw_cur_line) - len(raw_cur_line.lstrip())
    prev_indent = len(raw_prev_line) - len(raw_prev_line.lstrip())
    is_sub = prev_indent < cur_indent
    
    if is_sub:
        state = 'PathSub'
    else:
        state = 'PathDone'
        
    return state
    
def log_debug(cur, state, line):
    if DEBUG:
        print(cur, state, line)

def parse_path_with_state_machine(logs: list, cur: int):
    """
    state list:
    PathHeader, PathKeys, PathJoin, PathMJoin, PathOuter, PathInner
    PathWait, PathWait2, PathSub, PathDone
    """

    state = 'PathHeader'
    path_buffer = {}

    while state != 'PathDone' and cur < len(logs):
        line = logs[cur].strip()
        log_debug(cur, state, line)

        if state == 'PathHeader':
            _PATHHEADER_EXP = r'\ *(\w*)\((.*)\) required_outer \((\w*)\) rows=(\d*) cost=(\d*\.\d*)\.\.(\d*\.\d*)'
            _PATHHEADER_EXP_NOPARAM = r'\ *(\w*)\((.*)\) rows=(\d*) cost=(\d*\.\d*)\.\.(\d*\.\d*)'

            # get the header that is must be in the logs
            header = re.match(_PATHHEADER_EXP, line)
            node, relid, ro_relid, rows, startup_cost, total_cost = None, None, None, None, None, None
            if header:
                node, relid, ro_relid, rows, startup_cost, total_cost = header.groups()
            else:
                header = re.match(_PATHHEADER_EXP_NOPARAM, line)
                assert(header)
                node, relid, rows, startup_cost, total_cost = header.groups()

            path_buffer['node'] = node
            path_buffer['relid'] = relid
            if ro_relid:
                path_buffer['ro_relid'] = ro_relid
            path_buffer['rows'] = int(rows)
            path_buffer['startup_cost'] = float(startup_cost)
            path_buffer['total_cost'] = float(total_cost)
            
            state = 'PathWait'
            cur += 1

        elif state == 'PathWait':
            # a temp state to decide if it is PathKeys, PathJoin, PathMJoin, or PathCost
            _PATHKEYS_EXP = r'\ *pathkeys:\ (.*)'
            _CLAUSES_EXP = r'\ *clauses:(.*)'
            _MERGEJOIN_INFO_EXP = r'\ *sortouter=(\d) sortinner=(\d) materializeinner=(\d)'
            _COSTKEYS_EXP = r'\ *details:\ (.*)'

            if re.match(_PATHKEYS_EXP, line):
                state = 'PathKeys'
            elif re.match(_CLAUSES_EXP, line):
                state = 'PathJoin'
            elif re.match(_MERGEJOIN_INFO_EXP, line):
               state = 'PathMJoin'
            elif re.match(_COSTKEYS_EXP, line):
                state = 'PathCost'
            else:
                state = decide_next_state(logs, cur)
                
        elif state == 'PathKeys':
            _PATHKEYS_EXP = r'\ *pathkeys:\ (.*)'
            pathkeys = re.match(_PATHKEYS_EXP, line)
            assert(pathkeys)
            path_buffer['pathkeys'] = pathkeys.groups()[0].strip()
            
            state = 'PathWait'
            cur += 1
            
        elif state == 'PathJoin':
            _CLAUSES_EXP = r'\ *clauses:(.*)'
            clauses = re.match(_CLAUSES_EXP, line)
            assert(clauses)

            path_buffer['join'] = {
                'clauses': clauses.groups()[0].strip()
            }

            state = 'PathWait2'
            cur += 1

        elif state == 'PathMJoin':
            _MERGEJOIN_INFO_EXP = r'\ *sortouter=(\d) sortinner=(\d) materializeinner=(\d)'
            mj_info = re.match(_MERGEJOIN_INFO_EXP, line)
            assert(mj_info)

            sortouter, sortinner, matinner = mj_info.groups()
            path_buffer['sortouter'] = int(sortouter)
            path_buffer['sortinner'] = int(sortinner)
            path_buffer['matinner'] = int(matinner)

            state = 'PathOuter'
            cur += 1
            
        elif state == 'PathCost':
            if(path_buffer['node'] == 'SeqScan'):
                parse_seq_scan(line, path_buffer)
            elif(path_buffer['node'] == 'Gather'):
                parse_gather(line, path_buffer)
            elif(path_buffer['node'] == 'GatherMerge'):
                parse_gather_merge(line, path_buffer)
            elif(path_buffer['node'] == 'IdxScan'):
                parse_idx_scan(line, path_buffer)
            elif(path_buffer['node'] == 'BitmapHeapScan'):
                parse_bitmap_heap_scan(line, path_buffer)
            elif(path_buffer['node'] == 'SubqueryScan'):
                parse_subquery_scan(line, path_buffer)
            elif(path_buffer['node'] == 'Sort'):
                parse_sort(line, path_buffer)
            elif(path_buffer['node'] == 'IncrementalSort'):
                parse_incremental_sort(line, path_buffer)
            elif(path_buffer['node'] == 'NestLoop'):
                parse_nest_loop(line, path_buffer)
            elif(path_buffer['node'] == 'MergeJoin'):
                parse_merge_join(line, path_buffer)
            elif(path_buffer['node'] == 'HashJoin'):
                parse_hash_join(line, path_buffer)
            else:
                pass

            state = 'PathWait'
            cur += 1

        elif state == 'PathWait2':
            # a temp state to decide the new line is for MJoin or outer path
            _MERGEJOIN_INFO_EXP = r'\ *sortouter=(\d) sortinner=(\d) materializeinner=(\d)'

            if re.match(_MERGEJOIN_INFO_EXP, line):
                state = 'PathMJoin'
            else:
                state = 'PathOuter'

        elif state == 'PathOuter':
            outer, _cur = parse_path_with_state_machine(logs, cur)
            path_buffer['join']['outer'] = outer

            state = 'PathInner'
            cur = _cur

        elif state == 'PathInner':
            inner, _cur = parse_path_with_state_machine(logs, cur)
            path_buffer['join']['inner'] = inner 

            state = decide_next_state(logs, cur)
            cur = _cur

        elif state == 'PathSub':
            sub, _cur = parse_path_with_state_machine(logs, cur)
            path_buffer['sub'] = sub

            state = 'PathDone'
            cur = _cur

    return path_buffer, cur

def parse_with_state_machine(logs: list, cur: int, _START_SIGN: str, _END_SIGN: str):
    """
    state list:
        Start
        RelOptHeader, RelOptPathlist
        Path (PathHeader, PathKeys, PathJoin)
        Done
    """
    state = 'Start'
    buffer = {}

    while state != 'Done' and cur < len(logs):
        line = logs[cur].strip()
        log_debug(cur, state, line)

        if state == 'Start':
            if _START_SIGN in line:
                state = 'RelOptHeader'

            cur += 1

        elif state == 'RelOptHeader':
            _RELINFO_EXP = r'RELOPTINFO \((.*)\): rows=(\d*) width=(\d*)'

            # get relinfo if it exists in the logs
            relinfo = re.match(_RELINFO_EXP, line)
            
            if relinfo:
                relid, rows, width = relinfo.groups()
                buffer = {
                    'relid': relid,
                    'rows': int(rows),
                    'width': int(width)
                }

                state = 'Wait'
            
            cur += 1

        elif state == 'Wait':
            _PATH_LIST_EXP = 'path list:'
            _CHEAPESTPARAMPATH_LIST_EXP = 'cheapest parameterized paths:'
            _CHEAPESTSTARTUPPATH_EXP = 'cheapest startup path:'
            _CHEAPESTTOTALPATH_EXP = 'cheapest total path:'
            _APPENDPATH_EXP = 'append path:'

            if _PATH_LIST_EXP in line:
                state = 'PathList'
            elif _CHEAPESTPARAMPATH_LIST_EXP in line:
                state = 'CheapestParamPathList'
            elif _CHEAPESTSTARTUPPATH_EXP in line:
                state = 'CheapestStartupPath'
                cur += 1
            elif _CHEAPESTTOTALPATH_EXP in line:
                state = 'CheapestTotalPath'
                cur += 1
            elif _APPENDPATH_EXP in line:
                state = 'AppendRelOpt'
            elif _END_SIGN in line:
                state = 'Done'
                cur += 1
            else:
                cur += 1

        elif state == 'PathList':
            buffer['paths'] = []

            state = 'Path'
            cur += 1

        elif state == 'Path':
            _path_buffer, _cur = parse_path_with_state_machine(logs, cur)
            buffer['paths'].append(_path_buffer)

            state = 'PathContinue'
            cur = _cur

        elif state == 'PathContinue':
            strip = line.replace('\t', '').replace('\n', '').strip()
            if strip != '':
                state = 'Path'
            else:
                state = 'Wait'
                cur += 1

        elif state == 'CheapestParamPathList':
            buffer['cheapest_param_paths'] = []

            state = 'CheapestParamPath'
            cur += 1

        elif state == 'CheapestParamPath':
            _path_buffer, _cur = parse_path_with_state_machine(logs, cur)
            buffer['cheapest_param_paths'].append(_path_buffer)

            state = 'CheapestParamPathContinue'
            cur = _cur

        elif state == 'CheapestParamPathContinue':
            strip = line.replace('\t', '').replace('\n', '').strip()
            if strip != '':
                state = 'CheapestParamPath'
            else:
                state = 'Wait'
                cur += 1

        elif state == 'CheapestStartupPath':
            _path_buffer, _cur = parse_path_with_state_machine(logs, cur)
            buffer['cheapest_startup_paths'] = _path_buffer

            state = 'Wait'
            cur = _cur

        elif state == 'CheapestTotalPath':
            _path_buffer, _cur = parse_path_with_state_machine(logs, cur)
            buffer['cheapest_total_paths'] = _path_buffer

            state = 'Wait'
            cur = _cur

        elif state == 'AppendRelOpt':
            # custom start / end sign for the relinfo
            relinfo_start_sign = logs[cur].strip()
            relinfo_end_sign = 'append path done'
            _append_buf, _cur = parse_with_state_machine(
                logs, cur, relinfo_start_sign, relinfo_end_sign)
            cur = _cur

            # add it to the buffer
            if 'append' not in buffer:
                buffer['append'] = []
            buffer['append'].append(_append_buf)

            state = 'Wait'


    return buffer, cur

def get_base_path(log_lines: list, cur: int):
    _START_SIGN = '[JOVIS][BASE] set_rel_pathlist started'
    _END_SIGN = '[JOVIS][BASE] set_rel_pathlist done'
    return parse_with_state_machine(log_lines, cur, _START_SIGN, _END_SIGN)

def get_dp_path(log_lines: list, cur: int):
    _START_SIGN = '[JOVIS][DP] standard_join_search started'
    _END_SIGN = '[JOVIS][DP] standard_join_search done'
    return parse_with_state_machine(log_lines, cur, _START_SIGN, _END_SIGN)

def parse_seq_scan(line: str, buffer: dict):
    _SEQSCAN_DETAILS_EXP = r'\ *details: parallel_workers=(\d+) parallel_divisor=(\d+\.\d+) cpu_run_cost=(\d+\.\d+) disk_run_cost=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) baserel_tuples=(\d+\.\d+) pathtarget_cost=(\d+\.\d+) spc_seq_page_cost=(\d+\.\d+) baserel_pages=(\d+)'
    details = re.match(_SEQSCAN_DETAILS_EXP, line)
    
    if details:
        parallel_workers, parallel_divisor, cpu_run_cost, disk_run_cost, \
            cpu_per_tuple, baserel_tuples, pathtarget_cost, spc_seq_page_cost, baserel_pages \
                    = details.groups()
        
        buffer.update({
            'parallel_workers': int(parallel_workers),
            'parallel_divisor': float(parallel_divisor),
            'cpu_run_cost': float(cpu_run_cost),
            'disk_run_cost': float(disk_run_cost),
            'cpu_per_tuple': float(cpu_per_tuple),
            'baserel_tuples': float(baserel_tuples),
            'pathtarget_cost': float(pathtarget_cost),
            'spc_seq_page_cost': float(spc_seq_page_cost),
            'baserel_pages': int(baserel_pages),
        })
        
def parse_gather(line: str, buffer: dict):
    _GATHER_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) subpath_cost=(\d+\.\d+) parallel_tuple_cost=(\d+\.\d+)'
    details = re.match(_GATHER_DETAILS_EXP, line)
    
    if details:
        run_cost, subpath_cost, parallel_tuple_cost = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'subpath_cost': float(subpath_cost),
            'parallel_tuple_cost': float(parallel_tuple_cost)
        })
        
def parse_gather_merge(line: str, buffer: dict):
    _GATHERMERGE_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) input_startup_cost=(\d+\.\d+) input_total_cost=(\d+\.\d+) comparison_cost=(\d+\.\d+) logN=(\d+\.\d+) cpu_operator_cost=(\d+\.\d+) parallel_tuple_cost=(\d+\.\d+)'
    details = re.match(_GATHERMERGE_DETAILS_EXP, line)
    
    if details:
        run_cost, input_startup_cost, input_total_cost, comparison_cost, logN, cpu_operator_cost, parallel_tuple_cost = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'input_startup_cost': float(input_startup_cost),
            'input_total_cost': float(input_total_cost),
            'comparison_cost': float(comparison_cost),
            'logN': float(logN),
            'cpu_operator_cost': float(cpu_operator_cost),
            'parallel_tuple_cost': float(parallel_tuple_cost)
        })
        
def parse_idx_scan(line: str, buffer: dict):
    _IDXSCAN_DETAILS_EXP = r'\ *details: parallel_workers=(\d+) parallel_divisor=(\d+\.\d+) cpu_run_cost=(\d+\.\d+) disk_run_cost=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) baserel_tuples=(\d+\.\d+) pathtarget_cost=(\d+\.\d+) index_scan_cost=(\d+\.\d+) index_correlation=(-?\d+\.\d+) max_io_cost=(\d+\.\d+) min_io_cost=(\d+\.\d+)'
    details = re.match(_IDXSCAN_DETAILS_EXP, line)
    
    if details:
        parallel_workers, parallel_divisor, cpu_run_cost, disk_run_cost, \
            cpu_per_tuple, baserel_tuples, pathtarget_cost, index_scan_cost, \
                index_correlation, max_io_cost, min_io_cost \
                    = details.groups()
        
        buffer.update({
            'parallel_workers': int(parallel_workers),
            'parallel_divisor': float(parallel_divisor),
            'cpu_run_cost': float(cpu_run_cost),
            'disk_run_cost': float(disk_run_cost),
            'cpu_per_tuple': float(cpu_per_tuple),
            'baserel_tuples': float(baserel_tuples),
            'pathtarget_cost': float(pathtarget_cost),
            'index_scan_cost': float(index_scan_cost),
            'index_correlation': float(index_correlation),
            'max_io_cost': float(max_io_cost),
            'min_io_cost': float(min_io_cost)
        })


def parse_bitmap_heap_scan(line: str, buffer: dict):
    _BITMAPSCAN_DETAILS_EXP = r'\ *details: parallel_workers=(\d+) parallel_divisor=(\d+\.\d+) cpu_run_cost=(\d+\.\d+) disk_run_cost=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) tuples_fetched=(\d+\.\d+) pathtarget_cost=(\d+\.\d+) cost_per_page=(\d+\.\d+) pages_fetched=(\d+\.\d+)'
    details = re.match(_BITMAPSCAN_DETAILS_EXP, line)
    
    if details:
        parallel_workers, parallel_divisor, cpu_run_cost, disk_run_cost, \
            cpu_per_tuple, tuples_fetched, pathtarget_cost, cost_per_page, pages_fetched \
                = details.groups()
        
        buffer.update({
            'parallel_workers': int(parallel_workers),
            'parallel_divisor': float(parallel_divisor),
            'cpu_run_cost': float(cpu_run_cost),
            'disk_run_cost': float(disk_run_cost),
            'cpu_per_tuple': float(cpu_per_tuple),
            'tuples_fetched': float(tuples_fetched),
            'pathtarget_cost': float(pathtarget_cost),
            'cost_per_page': float(cost_per_page),
            'pages_fetched': float(pages_fetched)
        })
        
def parse_subquery_scan(line: str, buffer: dict):
    _SUBQSCAN_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) pathtarget_cost=(\d+\.\d+) subpath_total_cost=(\d+\.\d+) subpath_rows=(\d+\.\d+)'
    details = re.match(_SUBQSCAN_DETAILS_EXP, line)
    
    if details:
        run_cost, cpu_per_tuple, pathtarget_cost, subpath_total_cost, subpath_rows = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'cpu_per_tuple': float(cpu_per_tuple),
            'pathtarget_cost': float(pathtarget_cost),
            'subpath_total_cost': float(subpath_total_cost),
            'subpath_rows': float(subpath_rows)
        })
        
def parse_sort(line: str, buffer: dict):
    _SORT_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) cpu_operator_cost=(\d+\.\d+)'
    details = re.match(_SORT_DETAILS_EXP, line)
    
    if details:
        run_cost, cpu_operator_cost = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'cpu_operator_cost': float(cpu_operator_cost)
        })
        
def parse_incremental_sort(line: str, buffer: dict):
    _INCSORT_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) group_startup_cost=(\d+\.\d+) group_run_cost=(\d+\.\d+) group_input_run_cost=(\d+\.\d+) input_groups=(\d+\.\d+) cpu_tuple_cost=(\d+\.\d+) comparison_cost=(\d+\.\d+)'
    details = re.match(_INCSORT_DETAILS_EXP, line)
    
    if details:
        run_cost, group_startup_cost, group_run_cost, group_input_run_cost, \
            input_groups, cpu_tuple_cost, comparison_cost = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'group_startup_cost': float(group_startup_cost),
            'group_run_cost': float(group_run_cost),
            'group_input_run_cost': float(group_input_run_cost),
            'input_groups': float(input_groups),
            'cpu_tuple_cost': float(cpu_tuple_cost),
            'comparison_cost': float(comparison_cost)
        })
        
def parse_nest_loop(line: str, buffer: dict):
    _NESTLOOP_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) initial_outer_path_run_cost=(\d+\.\d+) initial_outer_path_rows=(\d+\.\d+) initial_inner_run_cost=(\d+\.\d+) initial_inner_rescan_start_cost=(\d+\.\d+) initial_inner_rescan_run_cost=(\d+\.\d+) is_early_stop=(\d+) has_indexed_join_quals=(\d+) inner_run_cost=(\d+\.\d+) inner_rescan_run_cost=(\d+\.\d+) outer_matched_rows=(\d+\.\d+) outer_unmatched_rows=(\d+\.\d+) inner_scan_frac=(\d+\.\d+) inner_path_rows=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) ntuples=(\d+\.\d+) cost_per_tuple=(\d+\.\d+)'
    details = re.match(_NESTLOOP_DETAILS_EXP, line)
    
    if details:
        run_cost, initial_outer_path_run_cost, initial_outer_path_rows, \
            initial_inner_run_cost, initial_inner_rescan_start_cost, initial_inner_rescan_run_cost, \
                is_early_stop, has_indexed_join_quals, inner_run_cost, inner_rescan_run_cost, \
                    outer_matched_rows, outer_unmatched_rows, inner_scan_frac, inner_path_rows, \
                        cpu_per_tuple, ntuples, cost_per_tuple = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'initial_outer_path_run_cost': float(initial_outer_path_run_cost),
            'initial_outer_path_rows': float(initial_outer_path_rows),
            'initial_inner_run_cost': float(initial_inner_run_cost),
            'initial_inner_rescan_start_cost': float(initial_inner_rescan_start_cost),
            'initial_inner_rescan_run_cost': float(initial_inner_rescan_run_cost),
            'is_early_stop': int(is_early_stop),
            'has_indexed_join_quals': int(has_indexed_join_quals),
            'inner_run_cost': float(inner_run_cost),
            'inner_rescan_run_cost': float(inner_rescan_run_cost),
            'outer_matched_rows': float(outer_matched_rows),
            'outer_unmatched_rows': float(outer_unmatched_rows),
            'inner_scan_frac': float(inner_scan_frac),
            'inner_path_rows': float(inner_path_rows),
            'cpu_per_tuple': float(cpu_per_tuple),
            'ntuples': float(ntuples),
            'cost_per_tuple': float(cost_per_tuple)
        })
        
def parse_merge_join(line: str, buffer: dict):
    _MERGEJOIN_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) initial_sort_path_run_cost=(\d+\.\d+) initial_outer_path_run_cost=(\d+\.\d+) initial_outer_sel=(\d+\.\d+) mat_inner_cost=(\d+\.\d+) bare_inner_cost=(\d+\.\d+) merge_qual_cost=(\d+\.\d+) outer_rows=(\d+\.\d+) inner_rows=(\d+\.\d+) outer_skip_rows=(\d+\.\d+) inner_skip_rows=(\d+\.\d+) rescanratio=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) mergejointuples=(\d+\.\d+) cost_per_tuple=(\d+\.\d+)'
    details = re.match(_MERGEJOIN_DETAILS_EXP, line)
    
    if details:
        run_cost, initial_sort_path_run_cost, initial_outer_path_run_cost, initial_outer_sel, \
            mat_inner_cost, bare_inner_cost, merge_qual_cost, outer_rows, inner_rows, \
                outer_skip_rows, inner_skip_rows, rescanratio, cpu_per_tuple, mergejointuples, cost_per_tuple = details.groups()
        
        buffer.update({
            'run_cost': float(run_cost),
            'initial_sort_path_run_cost': float(initial_sort_path_run_cost),
            'initial_outer_path_run_cost': float(initial_outer_path_run_cost),
            'initial_outer_sel': float(initial_outer_sel),
            'mat_inner_cost': float(mat_inner_cost),
            'bare_inner_cost': float(bare_inner_cost),
            'merge_qual_cost': float(merge_qual_cost),
            'outer_rows': float(outer_rows),
            'inner_rows': float(inner_rows),
            'outer_skip_rows': float(outer_skip_rows),
            'inner_skip_rows': float(inner_skip_rows),
            'rescanratio': float(rescanratio),
            'cpu_per_tuple': float(cpu_per_tuple),
            'mergejointuples': float(mergejointuples),
            'cost_per_tuple': float(cost_per_tuple)
        })
        
def parse_hash_join(line: str, buffer: dict):
    _HASHJOIN_DETAILS_EXP = r'\ *details: run_cost=(\d+\.\d+) initial_numbatches=(\d+) initial_outer_path_run_cost=(\d+\.\d+) initial_cpu_operator_cost=(\d+\.\d+) initial_num_hashclauses=(\d+) initial_outer_path_rows=(\d+\.\d+) initial_seq_page_cost=(\d+\.\d+) initial_innerpages=(\d+\.\d+) initial_outerpages=(\d+\.\d+) is_early_stop=(\d+) outer_matched_rows=(\d+\.\d+) outer_unmatched_rows=(\d+\.\d+) matched_bucket_rows=(\d+\.\d+) unmatched_bucket_rows=(\d+\.\d+) bucket_rows=(\d+\.\d+) hash_qual_cost=(\d+\.\d+) outer_path_rows=(\d+\.\d+) cpu_per_tuple=(\d+\.\d+) hashjointuples=(\d+\.\d+) cost_per_tuple=(\d+\.\d+)'
    details = re.match(_HASHJOIN_DETAILS_EXP, line)
    
    if details:
        run_cost, initial_numbatches, initial_outer_path_run_cost, initial_cpu_operator_cost, \
            initial_num_hashclauses, initial_outer_path_rows, initial_seq_page_cost, initial_innerpages, \
                initial_outerpages, is_early_stop, outer_matched_rows, outer_unmatched_rows, matched_bucket_rows, \
                    unmatched_bucket_rows, bucket_rows, hash_qual_cost, outer_path_rows, cpu_per_tuple, hashjointuples, cost_per_tuple = details.groups()
                
        buffer.update({
            'run_cost': float(run_cost),
            'initial_numbatches': int(initial_numbatches),
            'initial_outer_path_run_cost': float(initial_outer_path_run_cost),
            'initial_cpu_operator_cost': float(initial_cpu_operator_cost),
            'initial_num_hashclauses': int(initial_num_hashclauses),
            'initial_outer_path_rows': float(initial_outer_path_rows),
            'initial_seq_page_cost': float(initial_seq_page_cost),
            'initial_innerpages': float(initial_innerpages),
            'initial_outerpages': float(initial_outerpages),
            'is_early_stop': int(is_early_stop),
            'outer_matched_rows': float(outer_matched_rows),
            'outer_unmatched_rows': float(outer_unmatched_rows),
            'matched_bucket_rows': float(matched_bucket_rows),
            'unmatched_bucket_rows': float(unmatched_bucket_rows),
            'bucket_rows': float(bucket_rows),
            'hash_qual_cost': float(hash_qual_cost),
            'outer_path_rows': float(outer_path_rows),
            'cpu_per_tuple': float(cpu_per_tuple),
            'hashjointuples': float(hashjointuples),
            'cost_per_tuple': float(cost_per_tuple)
        })

def parse_geqo_with_state_machine(logs: list):
    """
    scan all logs and parse geqo data
    """
    cur = 0
    state = 'Init'
    buffer = {}
    tmpbuffer = {}

    while cur < len(logs):
        line = logs[cur].strip()
        log_debug(cur, state, line)

        if state == 'Init':
            _INIT_EXP = r'.*\[JOVIS\]\[GEQO\] GEQO selected (\d*) pool entries, best (\d*\.\d*), worst (\d*\.\d*)'
            initinfo = re.match(_INIT_EXP, line)
            if initinfo is None:
                cur += 1
                continue

            pool_size, best, worst = initinfo.groups()
            buffer['pool_size'] = int(pool_size)
            buffer['init'] = {'best': float(best), 'worst': float(worst)}
            buffer['gen'] = []

            state = 'Mapping'
            cur += 1
        
        elif state == 'Mapping':
            _MAPPING_EXP = r'\[JOVIS\]\[GEQO\] gene=(\d*) => relids=(.*)'
            mapinfo = re.match(_MAPPING_EXP, line)
            if mapinfo is None:
                if 'map' not in buffer:
                    # skip until reaching mapping lines
                    cur += 1
                    continue
                else:
                    # end of the state
                    state = 'Wait'
                    continue

            if 'map' not in buffer:
                buffer['map'] = {}

            gene, relids = mapinfo.groups()
            buffer['map'][gene] = relids
            cur += 1

        elif state == 'Wait':
            _GENERATION_EXP = r'.*\[GEQO\] *(\-?\d*).*Best: (.*)  Worst: (.*)  Mean: (.*)  Avg: (.*)'
            _OFFSPRING1_EXP = r'\[JOVIS\]\[GEQO\] parents=\[(\d*), (\d*)\]'
            if re.match(_GENERATION_EXP, line):
                state = 'Gen'
            elif re.match(_OFFSPRING1_EXP, line):
                state = 'Offspring'
            else:
                cur += 1


        elif state == 'Offspring':
            _OFFSPRING1_EXP = r'\[JOVIS\]\[GEQO\] parents=\[(\d*), (\d*)\]'

            offspringinfo = re.match(_OFFSPRING1_EXP, line)
            if offspringinfo:
                parent1, parent2 = offspringinfo.groups()
                tmpbuffer = {
                    'parents': [int(parent1), int(parent2)]
                }
                cur += 1
            else:
                # FIXME: This should be saperated into multiple states
                _GENERATION_EXP = r'.*\[GEQO\] *(\-?\d*).*Best: (.*)  Worst: (.*)  Mean: (.*)  Avg: (.*)'
                geninfo = re.match(_GENERATION_EXP, line)
                if geninfo:
                    # We should jump to the state 'Gen' cuz there is no newone_idx
                    state = 'Gen'
                    continue

                # Wait until we find newone_idx
                _OFFSPRING2_EXP = r'\[JOVIS\]\[GEQO\] newone_idx=(\d*)'
                offspring2info = re.match(_OFFSPRING2_EXP, line)
                if offspring2info is None:
                    cur += 1
                    continue

                newone_idx = offspring2info.groups()[0]
                tmpbuffer['newone_idx'] = int(newone_idx)
                cur += 1
                state = 'Gen'

        elif state == 'Gen':
            _GENERATION_EXP = r'.*\[GEQO\] *(\-?\d*).*Best: (.*)  Worst: (.*)  Mean: (.*)  Avg: (.*)'
            geninfo = re.match(_GENERATION_EXP, line)
            if geninfo is None:
                cur += 1
                continue

            gen_num, best, worst, mean, avg = geninfo.groups()
            buffer['gen'].append({
                'gen_num': int(gen_num),
                'best': float(best),
                'worst': float(worst),
                'mean': float(mean),
                'avg': float(avg),
                'pool': []
            })

            state = 'Pool'
            cur += 1

        elif state == 'Pool':
            _POOL_EXP = r'\[GEQO\] (\d*)\)(.*) (.*)'
            poolinfo = re.match(_POOL_EXP, line)
            if poolinfo is None:
                state = 'Wait'
                cur += 1
                continue

            population_num, gene, fitness = poolinfo.groups()

            cur_idx = len(buffer['gen'][-1]['pool'])
            data = {
                'population_num': int(population_num),
                'gene': gene.strip(),
                'fitness': float(fitness)
            }

            is_initial_pool = len(buffer['gen']) == 1
            if is_initial_pool is False:
                if 'newone_idx' in tmpbuffer:
                    if tmpbuffer['newone_idx'] == cur_idx:
                        data['parents'] = tmpbuffer['parents']
                    else :
                        data['prev_num'] = cur_idx if cur_idx < tmpbuffer['newone_idx'] \
                            else cur_idx - 1
                else:
                    data['prev_num'] = cur_idx

            buffer['gen'][-1]['pool'].append(data)

            cur += 1
            

    return buffer

def parse_geqo_path(logs: list) -> dict:
    # _GENE_EXP = r'\[JOVIS\]\[GEQO\]\[JOININFO\]((:? \d)*)'
    _GENE_EXP = r'\[JOVIS\]\[GEQO\]\[JOININFO\]\ gene=((:? \d)*)'

    cur = 0
    buffer = {}

    while cur < len(logs):
        line = logs[cur].strip()
        log_debug(cur, 'GEQO', line)

        geneinfo = re.match(_GENE_EXP, line)
        if geneinfo is None:
            cur += 1
            continue

        gene = geneinfo.groups()[0].strip()
        if gene in buffer:
            cur += 1
            continue

        # reuse this
        _buf, _cur = parse_with_state_machine(logs, cur, '[JOVIS][GEQO][JOININFO] gene=', '[JOVIS][GEQO][JOININFO] Done')
        buffer[gene] = _buf
        cur = _cur

    return buffer

def get_geqo_data(log_lines: list) -> dict:
    data = parse_geqo_with_state_machine(log_lines)
    data['reloptinfo'] = parse_geqo_path(log_lines)
    return data

def split_log_lines(log_lines):
    _MARK = '[JOVIS] split line'
    ret, for_items = [], []
    last = 0
    for idx, line in enumerate(log_lines):
        if _MARK not in line:
            continue

        ret.append(log_lines[last:idx])
        last = idx

        raw = line.split("RELOPTINFO")[1]
        relids = raw[raw.find("(")+1:raw.find(")")]
        for_items.append(relids)

    return ret, for_items

def process_log(log_lines):
    ret = {
        'type': 'dp',
        'base': [],
        'geqo': {},
        'dp': [] 
    }

    if '[GEQO]' in ''.join(log_lines):
        ret['type'] = 'geqo'

    _START_BASE_SIGN = '[JOVIS][BASE] set_rel_pathlist started'
    _START_DP_SIGN = '[JOVIS][DP] standard_join_search started'

    cur = 0
    # first pass for base and DP
    while cur < len(log_lines):
        line = log_lines[cur].strip()
        if _START_BASE_SIGN in line:
            base, _cur = get_base_path(log_lines, cur)
            ret['base'].append(base)
            cur = _cur - 1

        if _START_DP_SIGN in line:
            dp, _cur = get_dp_path(log_lines, cur)
            ret['dp'].append(dp)
            cur = _cur - 1

        cur += 1

    # second pass for GEQO
    if ret['type'] == 'geqo':
        ret['geqo'] = get_geqo_data(log_lines)

    return ret

def try_explain_analyze(in_query: str) -> str:
    hint_match = re.search(r'/\*\+.*?\*/', in_query, re.DOTALL)
    hint, query = '', ''
    
    if hint_match:
        hint = hint_match.group(0)
        query = in_query.replace(hint, '').strip()
    else:
        query = in_query.strip()
    
    if 'explain' not in query.lower():
        query = 'EXPLAIN (ANALYZE true, VERBOSE true, FORMAT JSON) ' + query

    return hint + ' ' + query
        
class QueryView(APIView):
    def post(self, request, format=None):
        # SQL 공격이 근본적으로 가능하므로, 절대 링크를 외부공개 하지 마세요.
        q = request.data.get('query', 'EXPLAIN SELECT \'Hello World\'')
        d = request.data.get('db', 'postgres')
        q = try_explain_analyze(q)

        # get query results
        try:
            conn = psycopg2.connect("host=localhost dbname={} user=postgres".format(d))    # Connect to your postgres DB
            cur = conn.cursor()         # Open a cursor to perform database operations

            clear_previous_log()
            
            cur.execute(q)              # Execute a query
            records = cur.fetchall()    # Retrieve query results

            log_lines = read_and_clear_log()
            # opt_data = process_log(log_lines)
            log_lines_list, for_items = split_log_lines(log_lines)
            ret = []
            for idx, logs in enumerate(log_lines_list):
                opt_data = process_log(logs)
                opt_data['for'] = for_items[idx]
                ret.append(opt_data)

            # return
            return Response({'query': q, 'result': records, 'optimizer': ret})
            # return Response({'query': q, 'result': records, 'optimizer': opt_data})
        except psycopg2.OperationalError as e:
            print(e)
            return Response({'error': str(e)})
        except psycopg2.errors.SyntaxError as e:
            print(e)
            return Response({'error': str(e)})
        except psycopg2.errors.UndefinedTable as e:
            print(e)
            return Response({'error': str(e)})
        except psycopg2.ProgrammingError as e:
            print(e)
            return Response({'error': str(e)})