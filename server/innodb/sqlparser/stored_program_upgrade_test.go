package sqlparser

import "testing"

func TestStoredProgramBlockPreservesNestedBodyBoundaries(t *testing.T) {
	block, err := ParseStoredProgramBlock("SET x = 'a;b'; BEGIN SET y = (1 + 2); END; SELECT x")
	if err != nil {
		t.Fatalf("ParseStoredProgramBlock() error = %v", err)
	}
	if len(block.Statements) != 3 || block.Statements[0].Kind != "set" || block.Statements[1].Kind != "begin" || block.Statements[2].Kind != "select" {
		t.Fatalf("statements = %#v", block.Statements)
	}
}

func TestStoredProgramUpgradeNestedBlocks(t *testing.T) {
	block, err := ParseStoredProgramBlock("IF x = 1 THEN SET y = 2; WHILE y < 3 DO SET y = y + 1; END WHILE; END IF")
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Statements) != 1 || block.Statements[0].Kind != "if" || block.Statements[0].Children == nil {
		t.Fatalf("block=%#v", block)
	}
}

func TestStoredProgramASTScopeAndHandlers(t *testing.T) {
	block, err := ParseStoredProgramBlock("DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; RETURN done")
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Statements) != 2 || block.Statements[0].HandlerAction != "CONTINUE" || block.Statements[0].HandlerCondition != "NOT FOUND" || block.Statements[1].ReturnExpr != "done" {
		t.Fatalf("statements=%#v", block.Statements)
	}
}

func TestStoredProgramBlockDoesNotTreatTriggerForEachRowAsLoop(t *testing.T) {
	block, err := ParseStoredProgramBlock("FOR EACH ROW SET NEW.value = 1")
	if err != nil {
		t.Fatal(err)
	}
	if len(block.Statements) != 1 || block.Statements[0].Kind != "trigger_body" {
		t.Fatalf("block=%#v", block)
	}
}
