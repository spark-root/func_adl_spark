import ast


class AttribToSubscriptTransformer(ast.NodeTransformer):
    """
    Converts
        Select(lambda event: event.MET_pt) into
        Select(lambda event: event['MET_pt'])
    """

    def visit_Attribute(self, node):
        ret = ast.Subscript(
            value=self.visit(node.value),
            slice=ast.Index(value=ast.Str(s=node.attr)),
            ctx=ast.Load(),
        )
        return ret
