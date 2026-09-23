package plan

// DNFConverter converts boolean expressions to disjunctive normal form:
// (a AND b) OR (c AND d). It is intentionally bounded because distributing
// boolean expressions can grow exponentially.
type DNFConverter struct {
	maxTerms int
}

func NewDNFConverter() *DNFConverter {
	return &DNFConverter{maxTerms: 100}
}

func (c *DNFConverter) SetMaxTerms(max int) {
	if max > 0 {
		c.maxTerms = max
	}
}

func (c *DNFConverter) ConvertToDNF(expr Expression) Expression {
	if expr == nil {
		return nil
	}
	return c.convert(expr)
}

func (c *DNFConverter) convert(expr Expression) Expression {
	operation, ok := expr.(*BinaryOperation)
	if !ok || (operation.Op != OpAnd && operation.Op != OpOr) {
		return expr
	}

	left := c.convert(operation.Left)
	right := c.convert(operation.Right)
	if operation.Op == OpOr {
		return joinBooleanItems(OpOr, append(booleanItems(OpOr, left), booleanItems(OpOr, right)...))
	}

	leftTerms := booleanItems(OpOr, left)
	rightTerms := booleanItems(OpOr, right)
	if len(leftTerms) == 0 || len(rightTerms) == 0 || len(leftTerms) > c.maxTerms/len(rightTerms) {
		return &BinaryOperation{Op: OpAnd, Left: left, Right: right}
	}
	terms := make([]Expression, 0, len(leftTerms)*len(rightTerms))
	for _, leftTerm := range leftTerms {
		for _, rightTerm := range rightTerms {
			factors := append(booleanItems(OpAnd, leftTerm), booleanItems(OpAnd, rightTerm)...)
			terms = append(terms, joinBooleanItems(OpAnd, factors))
		}
	}
	return joinBooleanItems(OpOr, terms)
}

func booleanItems(operator BinaryOp, expr Expression) []Expression {
	if operation, ok := expr.(*BinaryOperation); ok && operation.Op == operator {
		return append(booleanItems(operator, operation.Left), booleanItems(operator, operation.Right)...)
	}
	return []Expression{expr}
}

func joinBooleanItems(operator BinaryOp, items []Expression) Expression {
	if len(items) == 0 {
		return nil
	}
	result := items[0]
	for _, item := range items[1:] {
		result = &BinaryOperation{Op: operator, Left: result, Right: item}
	}
	return result
}
