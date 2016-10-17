import ply.lex as lex
import ply.yacc as yacc

import lexer

start = 'command'

def p_value_integer(p):
    'value : INTEGER'
    p[0] = {
        'type' : 'int',
        'value' : p[1]
    }

def p_value_float(p):
    'value : FLOAT'
    p[0] = {
        'type' : 'float',
        'value' : p[1]
    }

def p_value_string(p):
    'value : STRING'
    p[0] = {
        'type' : 'str',
        'value' : p[1]
    }

def p_value_variable(p):
    'value : QUESTIONMARK IDENTIFIER COLON IDENTIFIER'
    p[0] = {
        'type' : 'var',
        'kind' : p[4],
        'name' : p[2]
    }

def p_address_ipv4(p):
    'address : IPV4ADDRESS'
    p[0] = {
        'type' : 'address',
        'kind' : 'IPv4',
        'value' : p[1]
    }

def p_address_ipv6(p):
    'address : IPV6ADDRESS'
    p[0] = {
        'type' : 'address',
        'kind' : 'IPv6',
        'value' : p[1]
    }


def p_full_address(p):
    'fulladdress : address INTEGER'
    p[0] = {
        'type' : 'address',
        'kind' : p[1]['kind'],
        'value' : p[1]['value'],
        'port' : p[2]
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
        'type' : 'command',
        'args' : p[3],
        'name' : p[1]
    }


def p_command_network_simple(p):
    'simple_command : IDENTIFIER addresses'
    p[0] = {
        'type' : 'network_command',
        'args' : p[2],
        'name' : p[1]
    }

def p_command(p):
    'command : simple_command'
    p[0] = p[1]

def p_command_directive(p):
    'command : directive simple_command'
    p[0] = {
        'type' : p[2]['type'],
        'args' : p[2]['args'],
        'name' : p[2]['name'],
        'directive' : p[1]
    }

def p_error(p):
    print("Syntax error in input!")

_lexer = lex.lex(module=lexer)
parser = yacc.yacc()

def parse(input_string):
    _lexer.input(input_string)
    parse_tree = parser.parse(input_string, lexer=_lexer)
    return parse_tree
