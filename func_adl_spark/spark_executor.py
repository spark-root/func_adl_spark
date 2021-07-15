from .translation import generate_function


def ast_executor(ast):
    query_function = generate_function(ast)
    return query_function()
