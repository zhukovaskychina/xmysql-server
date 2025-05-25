package sqlparser

// Expr represents an expression.
type Expr interface {
	iExpr()
	// replace replaces any subexpression that matches
	// from with to. The implementation can use the
	// replaceExprs convenience function.
	replace(from, to Expr) bool

	//Eval(valueImpl innodb.Value) (innodb.Value,error)

	SQLNode
}

//func walkExpression() (Expr,error){
//
//}
