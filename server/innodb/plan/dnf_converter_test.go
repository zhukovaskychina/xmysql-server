package plan

import "testing"

func TestDNFConverterDistributesConjunctionOverDisjunction(t *testing.T) {
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}
	expr := &BinaryOperation{
		Op:    OpAnd,
		Left:  a,
		Right: &BinaryOperation{Op: OpOr, Left: b, Right: c},
	}

	got := NewDNFConverter().ConvertToDNF(expr)
	if got.String() != "((a AND b) OR (a AND c))" {
		t.Fatalf("ConvertToDNF() = %s", got.String())
	}
}

func TestDNFConverterRespectsExpansionLimit(t *testing.T) {
	converter := NewDNFConverter()
	converter.SetMaxTerms(1)
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}
	expr := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpOr, Left: a, Right: b},
		Right: c,
	}

	got := converter.ConvertToDNF(expr)
	if got.String() != "((a OR b) AND c)" {
		t.Fatalf("bounded ConvertToDNF() = %s", got.String())
	}
}

func TestExpressionNormalizerNormalizeToDNF(t *testing.T) {
	normalizer := NewExpressionNormalizer()
	a := &Column{Name: "a"}
	b := &Column{Name: "b"}
	c := &Column{Name: "c"}
	expr := &BinaryOperation{
		Op:    OpAnd,
		Left:  &BinaryOperation{Op: OpOr, Left: a, Right: b},
		Right: c,
	}

	if got := normalizer.NormalizeToDNF(expr).String(); got != "((a AND c) OR (b AND c))" {
		t.Fatalf("NormalizeToDNF() = %s", got)
	}
}
