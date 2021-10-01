import ast


class AttribToSubscriptTransformer(ast.NodeTransformer):
    """
    Converts
        Select(lambda event: event.MET_pt) into
        Select(lambda event: event['MET_pt'])
    """

    def visit_Attribute(self, node):
        return node
