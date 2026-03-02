from dataclasses import dataclass

from sqlglot import exp

from app.compiler.models import SQLAst, ValidatedAST


@dataclass
class SafetyViolationError(Exception):
    message: str

class SafetyEngine:
    """
    Implements a strict allow-list based structural validation on SQL.
    - Ensures root node is Select.
    - Blocks Subqueries and CTEs.
    - Enforces explicit DENY-LIST on dangerous AST nodes.
    - Allows only approved scalar functions.
    """

    # These must NEVER exist anywhere in the AST
    DENY_LIST = (
        exp.Anonymous,
        exp.Command,
        exp.Execute,
        exp.Transaction,
        exp.Hint,
        exp.Pragma,
        exp.Drop,
        exp.Create,
        exp.Alter,
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Grant,
        exp.Subquery,     # v1 Blocks subqueries
        exp.CTE,          # v1 Blocks CTEs (With clauses)
    )

    # Only these structural nodes are allowed beyond column lookups/literals
    # Note: the exact list may need tweaking during fuzzer tests, but we start strict.
    ALLOW_LIST = (
        exp.Select,
        exp.From,
        exp.Where,
        exp.Group,
        exp.Order,
        exp.Column,
        exp.Identifier,
        exp.Literal,
        exp.Boolean,
        exp.Table,
        exp.Join,
        exp.OnCondition,
        exp.Limit,
        exp.Offset,
        exp.Star,
        # Logical / Operators
        exp.And, exp.Or, exp.Not,
        exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE,
        exp.Like, exp.ILike, exp.In, exp.Between, exp.Is, exp.Null,
        exp.Paren,
        # Aggregations / Math
        exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max,
        exp.Coalesce, exp.Cast,
        # Types
        exp.DataType,
        # Alias
        exp.Alias, exp.ColumnPosition, exp.TableAlias, exp.Tuple
    )

    def validate(self, ast: SQLAst) -> ValidatedAST:
        """Runs the validation rules. Raises SafetyViolationError if blocked."""
        tree = ast.tree
        if tree is None:
            raise SafetyViolationError("AST tree is empty.")

        if not isinstance(tree, exp.Select):
            raise SafetyViolationError(
                f"Root node must be SELECT, found {type(tree).__name__}"
            )

        # Walk the entire tree and check every node
        for node in tree.walk():
            # Extract just the specific node instance from the walk
            # tuple (yields (node, parent, key))
            if isinstance(node, tuple):
               node_inst = node[0]
            else:
               node_inst = node

            node_type = type(node_inst)

            if issubclass(node_type, self.DENY_LIST):
                 raise SafetyViolationError(
                     f"Explicitly denied node type found: {node_type.__name__}"
                 )

            # Every node MUST explicitly be in our strict allow list
            if not issubclass(node_type, self.ALLOW_LIST):
                 raise SafetyViolationError(
                     f"Node type not in strict allow-list: {node_type.__name__}"
                 )

        return ValidatedAST(tree=tree)
