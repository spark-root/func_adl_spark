#!/usr/bin/env python

"""Tests for AttribToSubscript transformer"""


import unittest

import ast, qastle, astpretty

from func_adl_spark.transformers import AttribToSubscriptTransformer


class TestAttribToSubscriptTransformer(unittest.TestCase):
    def test_lambda_rewrite_complex(self):
        python_source = """Select(EventDataset('tests/scalars_tree_file.root', 'tree'), lambda event: event.MET_pt)"""
        query_python_raw_ast = ast.parse(python_source)
        query_python_ast = qastle.insert_linq_nodes(query_python_raw_ast)
        # You can dump the AST with
        # astpretty.pprint(query_python_ast, show_offsets=False)
        # Module(
        # 	body=[
        # 		Expr(
        # 			value=Select(
        # 				source=Call(
        # 					func=Name(id='EventDataset', ctx=Load()),
        # 					args=[
        # 						Str(s='tests/scalars_tree_file.root'),
        # 						Str(s='tree'),
        # 					],
        # 					keywords=[],
        # 				),
        # 				selector=Lambda(
        # 					args=arguments(
        # 						args=[arg(arg='event', annotation=None)],
        # 						vararg=None,
        # 						kwonlyargs=[],
        # 						kw_defaults=[],
        # 						kwarg=None,
        # 						defaults=[],
        # 					),
        # 					body=Attribute(
        # 						value=Name(id='event', ctx=Load()),
        # 						attr='MET_pt',
        # 						ctx=Load(),
        # 					),
        # 				),
        # 			),
        # 		),
        # 	],
        # )
        xform_python_ast = AttribToSubscriptTransformer().visit(query_python_ast)
        selector = xform_python_ast.body[0].value.selector.body
        selector_body_rep = astpretty.pformat(selector, show_offsets=False)
        desired_body = ast.Subscript(
            value=ast.Name(id="event", ctx=ast.Load()),
            slice=ast.Index(value=ast.Str(s="MET_pt")),
            ctx=ast.Load(),
        )
        desired_body_rep = astpretty.pformat(desired_body, show_offsets=False)
        self.assertEqual(
            selector_body_rep, desired_body_rep, "Ensuring bodies are the same"
        )

    def test_lambda_rewrite_double_attr(self):
        python_source = """Select(EventDataset('tests/scalars_tree_file.root', 'tree'), lambda event: event.MET_pt.foo)"""
        query_python_raw_ast = ast.parse(python_source)
        query_python_ast = qastle.insert_linq_nodes(query_python_raw_ast)
        # You can dump the AST with
        # astpretty.pprint(query_python_ast, show_offsets=False)
        # Module(
        # 	body=[
        # 		Expr(
        # 			value=Select(
        # 				source=Call(
        # 					func=Name(id='EventDataset', ctx=Load()),
        # 					args=[
        # 						Str(s='tests/scalars_tree_file.root'),
        # 						Str(s='tree'),
        # 					],
        # 					keywords=[],
        # 				),
        # 				selector=Lambda(
        # 					args=arguments(
        # 						args=[arg(arg='event', annotation=None)],
        # 						vararg=None,
        # 						kwonlyargs=[],
        # 						kw_defaults=[],
        # 						kwarg=None,
        # 						defaults=[],
        # 					),
        # 					body=Attribute(
        # 						value=Name(id='event', ctx=Load()),
        # 						attr='MET_pt',
        # 						ctx=Load(),
        # 					),
        # 				),
        # 			),
        # 		),
        # 	],
        # )
        xform_python_ast = AttribToSubscriptTransformer().visit(query_python_ast)
        selector = xform_python_ast.body[0].value.selector.body
        selector_body_rep = astpretty.pformat(selector, show_offsets=False)
        desired_body = ast.Subscript(
            value=ast.Subscript(
                value=ast.Name(id="event", ctx=ast.Load()),
                slice=ast.Index(
                    value=ast.Str(s="MET_pt"),
                ),
                ctx=ast.Load(),
            ),
            slice=ast.Index(
                value=ast.Str(s="foo"),
            ),
            ctx=ast.Load(),
        )

        desired_body_rep = astpretty.pformat(desired_body, show_offsets=False)
        self.assertEqual(
            selector_body_rep, desired_body_rep, "Ensuring bodies are the same"
        )


if __name__ == "__main__":
    unittest.main()
