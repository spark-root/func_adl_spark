import ast


class LambdaSelectVisitor(ast.NodeVisitor):
    """
    Given an AST, find all the attributes requested from a certain ID. This is
    used to be able to see which attributes need to be declared to Spark
    beforehand
    """

    def __init__(self, lambda_id):
        self.lambda_id = lambda_id
        self.attributes = {}

    def visit_Attribute(self, node):
        if node.value.id == self.lambda_id:
            self.attributes[node.attr] = 1

    def get_accessed_columns(self):
        return set(self.attributes.keys())


class RebindLambdaToSparkDFTransformer(ast.NodeTransformer):
    """
    Converts
        Select(lambda event: event.MET_pt) into
        Select(lambda MET_pt:
                (event = dict()
                 event['MET_pt'] = MET_pt
                 return (lambda event: event.MET_pt)))
    so Spark can consume it
    """

    def __init__(self):
        self._id_scopes = {}

    def getNameForAttribute(self, arg, attrib):
        """
        Given events.MET_pt, return _events_MET_pt
        Need to figure out a more robust way to do this
        """
        return "_%s_%s" % (arg, attrib)

    def visit_Select(self, node):
        if type(node.selector) is not ast.Lambda:
            raise TypeError(
                "Argument to Select() must be a lambda function, found " + node.selector
            )
        if len(node.selector.args.args) != 1:
            raise TypeError(
                "Lambda function in Select() must have exactly one argument, found "
                + len(node.selector.args.args)
            )

        # The argument passed into the lambda from the user, e.g.
        # the "event" in "lambda event: event.XYZ"
        lambda_arg = node.selector.args.args[0].arg

        # Now we need to find out which attributes of the event the lambda uses
        visitor = LambdaSelectVisitor(lambda_arg)
        visitor.visit(node.selector.body)
        # Store the accessed columns in the node so we can use it later
        node.accessed_columns = cols = sorted(list(visitor.get_accessed_columns()))
        node.renamed_columns = [self.getNameForAttribute(lambda_arg, x) for x in cols]

        # I think that the "annotation" field is spurious, but for some
        # reason, needed to be able to astpretty.pprint() the args
        args = ast.arguments(
            args=[
                ast.arg(arg=x, annotation=ast.Constant(value=""))
                for x in node.renamed_columns
            ]
        )

        replaceTransform = InternalRebindLambdaToSparkDFTransformer(
            lambda_arg, node.accessed_columns, node.renamed_columns
        )
        body = replaceTransform.visit(node.selector.body)
        node.selector = ast.Lambda(args=args, body=body)
        return node


class InternalRebindLambdaToSparkDFTransformer(ast.NodeTransformer):
    """
    Only used in the context of a Select, SelectMany, etc... to replace
    Lambdas. Importantly: if the lambda has another lambda inside of it
    it won't replace the inner values as well
    """

    def __init__(self, lambda_arg, accessed_columns, renamed_columns):
        self.lambda_arg = lambda_arg
        self.accessed_columns = accessed_columns
        self.renamed_columns = renamed_columns

    def getNameForAttribute(self, arg, attrib):
        """
        Need to figure out a more robust way to do this
        """
        return "_%s_%s" % (arg, attrib)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == self.lambda_arg:
            return ast.Name(
                id=self.getNameForAttribute(self.lambda_arg, node.attr), ctx=node.ctx
            )
        else:
            return node
