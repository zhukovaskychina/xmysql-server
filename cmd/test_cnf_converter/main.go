package main

import (
	"fmt"
	"github.com/zhukovaskychina/xmysql-server/server/innodb/plan"
)

func main() {
	fmt.Println("=== CNF转换器测试 ===\n")

	converter := plan.NewCNFConverter()

	// 测试1: 双重否定消除
	fmt.Println("测试1: 双重否定消除")
	fmt.Println("输入: NOT (NOT a)")
	inner := &plan.Column{Name: "a"}
	notInner := &plan.NotExpression{Operand: inner}
	notNotInner := &plan.NotExpression{Operand: notInner}
	result1 := converter.ConvertToCNF(notNotInner)
	fmt.Printf("输出: %s\n\n", result1.String())

	// 测试2: 德摩根定律 - NOT (a AND b)
	fmt.Println("测试2: 德摩根定律 - NOT (a AND b)")
	fmt.Println("输入: NOT (a AND b)")
	a := &plan.Column{Name: "a"}
	b := &plan.Column{Name: "b"}
	andExpr := &plan.BinaryOperation{Op: plan.OpAnd, Left: a, Right: b}
	notAndExpr := &plan.NotExpression{Operand: andExpr}
	result2 := converter.ConvertToCNF(notAndExpr)
	fmt.Printf("输出: %s\n", result2.String())
	fmt.Println("期望: (NOT (a)) OR (NOT (b))\n")

	// 测试3: 德摩根定律 - NOT (a OR b)
	fmt.Println("测试3: 德摩根定律 - NOT (a OR b)")
	fmt.Println("输入: NOT (a OR b)")
	a2 := &plan.Column{Name: "a"}
	b2 := &plan.Column{Name: "b"}
	orExpr := &plan.BinaryOperation{Op: plan.OpOr, Left: a2, Right: b2}
	notOrExpr := &plan.NotExpression{Operand: orExpr}
	result3 := converter.ConvertToCNF(notOrExpr)
	fmt.Printf("输出: %s\n", result3.String())
	fmt.Println("期望: (NOT (a)) AND (NOT (b))\n")

	// 测试4: 运算符取反
	fmt.Println("测试4: 运算符取反 - NOT (age > 18)")
	fmt.Println("输入: NOT (age > 18)")
	age := &plan.Column{Name: "age"}
	ageGt18 := &plan.BinaryOperation{
		Op:    plan.OpGT,
		Left:  age,
		Right: &plan.Constant{Value: int64(18)},
	}
	fmt.Printf("谓词类型: %T, Op: %v\n", ageGt18, ageGt18.Op)
	notAgeGt18 := &plan.NotExpression{Operand: ageGt18}
	result4 := converter.ConvertToCNF(notAgeGt18)
	fmt.Printf("输出: %s (%T)\n", result4.String(), result4)
	fmt.Println("期望: age <= 18\n")

	// 测试5: 简单分配律 - a OR (b AND c)
	fmt.Println("测试5: 简单分配律 - a OR (b AND c)")
	fmt.Println("输入: a OR (b AND c)")
	a3 := &plan.Column{Name: "a"}
	b3 := &plan.Column{Name: "b"}
	c3 := &plan.Column{Name: "c"}
	andBC := &plan.BinaryOperation{Op: plan.OpAnd, Left: b3, Right: c3}
	orABC := &plan.BinaryOperation{Op: plan.OpOr, Left: a3, Right: andBC}
	result5 := converter.ConvertToCNF(orABC)
	fmt.Printf("输出: %s\n", result5.String())
	fmt.Println("期望: (a OR b) AND (a OR c)\n")

	// 测试6: 复杂分配律 - (a AND b) OR (c AND d)
	fmt.Println("测试6: 复杂分配律 - (a AND b) OR (c AND d)")
	fmt.Println("输入: (a AND b) OR (c AND d)")
	a4 := &plan.Column{Name: "a"}
	b4 := &plan.Column{Name: "b"}
	c4 := &plan.Column{Name: "c"}
	d4 := &plan.Column{Name: "d"}
	andAB := &plan.BinaryOperation{Op: plan.OpAnd, Left: a4, Right: b4}
	andCD := &plan.BinaryOperation{Op: plan.OpAnd, Left: c4, Right: d4}
	orABCD := &plan.BinaryOperation{Op: plan.OpOr, Left: andAB, Right: andCD}
	result6 := converter.ConvertToCNF(orABCD)
	fmt.Printf("输出: %s\n", result6.String())
	fmt.Println("期望: (a OR c) AND (a OR d) AND (b OR c) AND (b OR d)\n")

	// 测试7: 复杂表达式 - NOT ((age > 18 AND city = 'Beijing') OR status = 'inactive')
	fmt.Println("测试7: 复杂表达式转换")
	fmt.Println("输入: NOT ((age > 18 AND city = 'Beijing') OR status = 'inactive')")
	age2 := &plan.Column{Name: "age"}
	city := &plan.Column{Name: "city"}
	status := &plan.Column{Name: "status"}

	ageGt18_2 := &plan.BinaryOperation{
		Op:    plan.OpGT,
		Left:  age2,
		Right: &plan.Constant{Value: int64(18)},
	}
	cityEqBeijing := &plan.BinaryOperation{
		Op:    plan.OpEQ,
		Left:  city,
		Right: &plan.Constant{Value: "Beijing"},
	}
	statusInactive := &plan.BinaryOperation{
		Op:    plan.OpEQ,
		Left:  status,
		Right: &plan.Constant{Value: "inactive"},
	}

	andAgeCity := &plan.BinaryOperation{Op: plan.OpAnd, Left: ageGt18_2, Right: cityEqBeijing}
	orExpr2 := &plan.BinaryOperation{Op: plan.OpOr, Left: andAgeCity, Right: statusInactive}
	notExpr := &plan.NotExpression{Operand: orExpr2}
	result7 := converter.ConvertToCNF(notExpr)
	fmt.Printf("输出: %s\n", result7.String())
	fmt.Println("期望: (age <= 18 OR city != 'Beijing') AND status != 'inactive'\n")

	// 测试8: 提取合取项
	fmt.Println("测试8: 提取合取项 - a AND b AND c")
	a5 := &plan.Column{Name: "a"}
	b5 := &plan.Column{Name: "b"}
	c5 := &plan.Column{Name: "c"}
	andBC2 := &plan.BinaryOperation{Op: plan.OpAnd, Left: b5, Right: c5}
	andABC2 := &plan.BinaryOperation{Op: plan.OpAnd, Left: a5, Right: andBC2}
	conjuncts := converter.ExtractConjuncts(andABC2)
	fmt.Printf("提取到 %d 个合取项:\n", len(conjuncts))
	for i, conj := range conjuncts {
		fmt.Printf("  %d: %s\n", i+1, conj.String())
	}
	fmt.Println()

	// 测试9: 提取析取项
	fmt.Println("测试9: 提取析取项 - a OR b OR c")
	a6 := &plan.Column{Name: "a"}
	b6 := &plan.Column{Name: "b"}
	c6 := &plan.Column{Name: "c"}
	orBC := &plan.BinaryOperation{Op: plan.OpOr, Left: b6, Right: c6}
	orABC2 := &plan.BinaryOperation{Op: plan.OpOr, Left: a6, Right: orBC}
	disjuncts := converter.ExtractDisjuncts(orABC2)
	fmt.Printf("提取到 %d 个析取项:\n", len(disjuncts))
	for i, disj := range disjuncts {
		fmt.Printf("  %d: %s\n", i+1, disj.String())
	}
	fmt.Println()

	fmt.Println("=== 所有测试完成 ===")
}
