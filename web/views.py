import psycopg2
import os
import time
import re

from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response

from backend.settings import PG_LOG_FILE, PG_LOG_BACKUP_DIR

def clear_previous_log():
    os.system(f"cp {PG_LOG_FILE} {PG_LOG_BACKUP_DIR}/{time.time()}_prev")
    os.system(f"echo '' > {PG_LOG_FILE}")

def read_and_clear_log():
    filename = f"{PG_LOG_BACKUP_DIR}/{time.time()}_pq"
    os.system(f"cp {PG_LOG_FILE} {filename}")
    os.system(f"echo '' > {PG_LOG_FILE}")

    ret = None
    with open(filename, 'r') as f:
        ret = f.readlines()

    return ret


def parse_path_with_state_machine(logs: list, cur: int):
    """
    state list:
    PathHeader, PathKeys, PathJoin, PathMJoin, PathOuter, PathInner
    PathWait, PathWait2
    PathDone
    """

    state = 'PathHeader'
    path_buffer = {}

    while state != 'PathDone' and cur < len(logs):
        line = logs[cur].strip()
        print(cur, state, line)
        #input()

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
            # a temp state to decide if it is PathKeys, PathJoin, or PathMJoin
            _PATHKEYS_EXP = r'\ *pathkeys:\ (.*)'
            _CLAUSES_EXP = r'\ *clauses:(.*)'
            #_MERGEJOIN_INFO_EXP = r'\ *sortouter=(\d) sortinner=(\d) materializeinner=(\d)'

            if re.match(_PATHKEYS_EXP, line):
                state = 'PathKeys'
            elif re.match(_CLAUSES_EXP, line):
                state = 'PathJoin'
            #elif re.match(_MERGEJOIN_INFO_EXP, line):
            #    state = 'PathMJoin'
            else:
                # check indentation width
                raw_cur_line, raw_prev_line = logs[cur].replace('\t', '    '), logs[cur-1].replace('\t', '    ')
                cur_indent = len(raw_cur_line) - len(raw_cur_line.lstrip())
                prev_indent = len(raw_prev_line) - len(raw_prev_line.lstrip())
                is_sub = prev_indent < cur_indent
                if is_sub:
                    state = 'PathSub'
                else:
                    state = 'PathDone'

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

            outerkeys_exist, innerkeys_exist, m_inner_exist = mj_info.groups()
            path_buffer['join']['mergejoin_info'] = {
                'outerkeys_exist': outerkeys_exist,
                'innerkeys_exist': innerkeys_exist,
                'm_inner_exist': m_inner_exist
            }

            state = 'PathOuter'
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

            state = 'PathWait3'
            cur = _cur

        elif state == 'PathSub':
            sub, _cur = parse_path_with_state_machine(logs, cur)
            path_buffer['sub'] = sub

            state = 'PathDone'
            cur = _cur

        elif state == 'PathWait3':
            raw_cur_line, raw_prev_line = logs[cur].replace('\t', '    '), logs[cur-1].replace('\t', '    ')
            cur_indent = len(raw_cur_line) - len(raw_cur_line.lstrip())
            prev_indent = len(raw_prev_line) - len(raw_prev_line.lstrip())
            is_super = prev_indent > cur_indent
            if is_super:
                state = 'PathDone'
            else:
                state = 'PathSub'

    return path_buffer, cur

    

def parse_with_state_machine(logs: list, cur: int, _START_SIGN: str, _END_SIGN: str):
    """
    state list:
        Start
        RelOptHeader, RelOptPathlist
        Path (PathHeader, PathKeys, PathJoin, PathMJoin)
        Done
    """
    state = 'Start'
    buffer = {}

    while state != 'Done' and cur < len(logs):
        line = logs[cur].strip()
        print(cur, state, line)

        if state == 'Start':
            if _START_SIGN in line:
                state = 'RelOptHeader'

            cur += 1

        elif state == 'RelOptHeader':
            _RELINFO_EXP = r'RELOPTINFO \((.*)\): rows=(\d*) width=(\d*)'

            # get relinfo that is must be in the logs
            relinfo = re.match(_RELINFO_EXP, line)
            assert(relinfo)

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

    return buffer, cur


def get_base_path(log_lines: list, cur: int):
    _START_SIGN = '[VPQO][BASE] set_rel_pathlist started'
    _END_SIGN = '[VPQO][BASE] set_rel_pathlist done'
    return parse_with_state_machine(log_lines, cur, _START_SIGN, _END_SIGN)


def get_dp_path(log_lines: list, cur: int):
    _START_SIGN = '[VPQO][DP] standard_join_search started'
    _END_SIGN = '[VPQO][DP] standard_join_search done'
    return parse_with_state_machine(log_lines, cur, _START_SIGN, _END_SIGN)

def parse_geqo_with_state_machine(logs: list):
    """
    scan all logs and parse geqo data
    """
    cur = 0
    state = 'Init'
    buffer = {}

    while cur < len(logs):
        line = logs[cur].strip()
        print(cur, state, line)

        if state == 'Init':
            _INIT_EXP = r'.*\[VPQO\]\[GEQO\] GEQO selected (\d*) pool entries, best (\d*\.\d*), worst (\d*\.\d*)'
            initinfo = re.match(_INIT_EXP, line)
            if initinfo is None:
                cur += 1
                continue

            pool_size, best, worst = initinfo.groups()
            buffer['pool_size'] = int(pool_size)
            buffer['init'] = {'best': float(best), 'worst': float(worst)}
            buffer['gen'] = []

            state = 'Gen'
            cur += 1

        elif state == 'Gen':
            _GENERATION_EXP = r'.*\[GEQO\] *(\d*).*Best: (\d*\.\d*)  Worst: (\d*\.\d*)  Mean: (\d*\.\d*)  Avg: (\d*\.\d*)'
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
            _POOL_EXP = r'\[GEQO\] (\d*)\)(.*) (\d*\.\d*)'
            poolinfo = re.match(_POOL_EXP, line)
            if poolinfo is None:
                state = 'Gen'
                cur += 1
                continue

            population_num, gene, fitness = poolinfo.groups()
            buffer['gen'][-1]['pool'].append({
                'population_num': int(population_num),
                'gene': gene.strip(),
                'fitness': float(fitness)
            })

            cur += 1

    return buffer


def get_geqo_data(log_lines: list) -> dict:
    return parse_geqo_with_state_machine(log_lines)

def process_log(log_lines):
    ret = {
        'type': 'dp',
        'base': [],
        'geqo': {},
        'dp': [] 
    }

    if '[GEQO]' in ''.join(log_lines):
        ret['type'] = 'geqo'

    _START_BASE_SIGN = '[VPQO][BASE] set_rel_pathlist started'
    _START_DP_SIGN = '[VPQO][DP] standard_join_search started'

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
        
class QueryView(APIView):
    def post(self, request, format=None):
        # SQL 공격이 근본적으로 가능하므로, 절대 링크를 외부공개 하지 마세요.
        q = request.data.get('query', 'EXPLAIN SELECT \'Hello World\'')
        
        # get query results
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=mysecretpassword")    # Connect to your postgres DB
        cur = conn.cursor()     # Open a cursor to perform database operations

        clear_previous_log()

        cur.execute(q)        # Execute a query
        records = cur.fetchall()     # Retrieve query results

        log_lines = read_and_clear_log()
        opt_data = process_log(log_lines)

        # return
        return Response({'query': q, 'result': str(records), 'optimizer': opt_data})
