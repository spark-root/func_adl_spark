#!/usr/bin/env python

"""Tests for `func_adl_spark` package."""


import unittest

from func_adl_spark import func_adl_spark

import ast, qastle, astpretty, ast_scope
from func_adl_spark.spark_translation import (
    generate_python_source,
    python_ast_to_python_source,
)
from func_adl_spark.transformers import (
    LambdaSelectVisitor,
    RebindLambdaToSparkDFTransformer,
)


class TestRebind_lambda_to_spark_df_transformer(unittest.TestCase):
    def test_lambda_visitor_simple(self):
        python_source = """lambda event: event.MET_pt"""

        query_python_raw_ast = ast.parse(python_source)
        query_python_ast = qastle.insert_linq_nodes(query_python_raw_ast)
        visitor = LambdaSelectVisitor("event")
        visitor.visit(query_python_raw_ast)
        self.assertEqual(visitor.get_accessed_columns(), {"MET_pt"}, "Comparing sets")

        lambda_python_ast = RebindLambdaToSparkDFTransformer().visit(query_python_ast)
        query_python_raw_annotate = ast_scope.annotate(lambda_python_ast)

    def test_lambda_visitor_complex(self):
        python_source = """lambda event: event.MET_pt + event.MET_dx - unknown.TestXX"""

        query_python_raw_ast = ast.parse(python_source)
        query_python_ast = qastle.insert_linq_nodes(query_python_raw_ast)
        visitor = LambdaSelectVisitor("event")
        visitor.visit(query_python_raw_ast)
        self.assertEqual(
            visitor.get_accessed_columns(),
            {"MET_pt", "MET_dx"},
            "Shouldn't find random attributes",
        )

    def test_lambda_rewrite_complex(self):
        python_source = """Select(EventDataset('tests/scalars_tree_file.root', 'tree'), lambda event: event.MET_pt + event.MET_dxy)"""
        query_python_raw_ast = ast.parse(python_source)
        query_python_ast = qastle.insert_linq_nodes(query_python_raw_ast)
        lambda_python_ast = RebindLambdaToSparkDFTransformer().visit(query_python_ast)

        # This is the dump of the AST at this point and we want the Select
        # Module(
        #     body=[
        #         Expr(
        #             value=Select(
        #                 source=Call(
        #                     func=Name(id='EventDataset', ctx=Load()),
        #                     args=[
        #                         Str(s='tests/scalars_tree_file.root'),
        #                         Str(s='tree'),
        #                     ],
        #                     keywords=[],
        #                 ),
        #                 selector=Lambda(
        #                     args=arguments(
        selector = lambda_python_ast.body[0].value.selector
        selector_args_rep = astpretty.pformat(selector.args, show_offsets=False)
        selector_body_rep = astpretty.pformat(selector.body, show_offsets=False)

        desired_args = ast.arguments(
            args=[
                ast.arg(
                    arg="_event_MET_dxy", annotation=ast.Constant(value="")
                ),
                ast.arg(
                    arg="_event_MET_pt", annotation=ast.Constant(value="")
                ),
            ]
        )
        desired_args_rep = astpretty.pformat(desired_args, show_offsets=False)
        self.assertEqual(
            desired_args_rep, selector_args_rep, "Ensuring args are the same"
        )

        desired_body = ast.BinOp(
            left=ast.Name(id="_event_MET_pt", ctx=ast.Load()),
            right=ast.Name(id="_event_MET_dxy", ctx=ast.Load()),
            op=ast.Add(),
        )
        desired_body_rep = astpretty.pformat(desired_body, show_offsets=False)
        self.assertEqual(
            desired_body_rep, selector_body_rep, "Ensuring bodies are the same"
        )


if __name__ == "__main__":
    unittest.main()
