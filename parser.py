import ply.lex as lex
import ply.yacc as yacc

import lexer
from lexer import tokens

start = 'command'


def p_value_integer(p):
    'value : INTEGER'
    p[0] = {
        'type': 'value',
        'value': p[1]
    }


def p_value_float(p):
    'value : FLOAT'
    p[0] = {
        'type': 'value',
        'value': p[1]
    }


def p_value_string(p):
    'value : STRING'
    p[0] = {
        'type': 'value',
        'value': p[1]
    }


def p_value_variable(p):
    'value : QUESTIONMARK IDENTIFIER COLON IDENTIFIER'
    p[0] = {
        'type': 'variable',
        'kind': p[4],
        'name': p[2]
    }


def p_address_ipv4(p):
    'address : IPV4ADDRESS'
    p[0] = p[1]


def p_address_ipv6(p):
    'address : IPV6ADDRESS'
    p[0] = p[1]


def p_full_address(p):
    'fulladdress : address INTEGER'
    p[0] = {
        'host': p[1],
        'port': p[2]
    }


def p_addresses(p):
    'addresses : fulladdress addresses'
    p[0] = [p[1]] + p[2]


def p_addresses_last(p):
    'addresses : fulladdress'
    p[0] = [p[1]]


def p_params(p):
    'params : value COMMA params'
    p[0] = [p[1]] + p[3]


def p_params_last(p):
    'params : value'
    p[0] = [p[1]]


def p_optparams(p):
    'optparams : params'
    p[0] = p[1]


def p_optparams_empty(p):
    'optparams : '
    p[0] = [ ]


def p_directive(p):
    'directive : LBRACKET IDENTIFIER RBRACKET'
    p[0] = p[2]


def p_command_simple(p):
    'simple_command : IDENTIFIER LPAREN optparams RPAREN'
    p[0] = {
        'command': p[1],
        'args': p[3]
    }


def p_command_network_simple(p):
    'simple_command : IDENTIFIER addresses'
    p[0] = {
        'command': p[1],
        'args': p[2]
    }


def p_command(p):
    'command : simple_command'
    p[0] = p[1]


def p_command_directive(p):
    'command : directive simple_command'
    p[0] = {
        'command': p[2]['command'],
        'args': p[2]['args'],
        'directive': p[1]
    }


def p_error(p):
    print("Syntax error in input!")

_lexer = lex.lex(module=lexer)
parser = yacc.yacc()


def parse(input_string):
    _lexer.input(input_string)
    parse_tree = parser.parse(input_string, lexer=_lexer)
    return parse_tree


# s1 = 'rd("Hello", 23)'
# s2 = 'out(?x:int, ?y:float)'
# s3 = 'in(?x:int, "hello", 2, 4.5)'
# s4 = '[exec] rd("Hello", ?x:int)'
# s5 = '[exec] out()'
# s6 = 'rd("Hello, world!")'
#
# for s in [s1, s2, s3, s4, s5, s6]:
#     print(s)
#     print(parse(s))