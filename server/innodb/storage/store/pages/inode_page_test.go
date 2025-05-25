package pages

import (
	"fmt"
	"testing"
	"unsafe"
)

type Student struct {
}

func TestINodePoint(t *testing.T) {

	//使用自动推导类型创建一个age变量
	age := 18

	//我们将age变量的内存地址赋值给指针变量p1
	p1 := &age

	/**
	  我们可以使用取值运算符("*")来获取指针变量的值。
	  %p:
	      是一个占位符,表示输出一个十六进制地址格式。
	  %d:
	      是一个占位符，表示输出一个整型。
	  \n:
	      表示换行符。
	  *:
	      取值运算符，可以将指针变量中的保存的数据取出来。换句话说，可以将一段内存地址保存的值取出来。
	*/
	fmt.Printf("age的内存地址是:%p,age的值是:%d\n", p1, *p1)

	//我们可以通过指针间接修改变量的值
	*p1 = 27
	fmt.Printf("age的内存地址是:%p,age的值是:%d\n", p1, age)
	fmt.Println(0xc000122240)
	var b uint16
	b = 2550
	fmt.Println(unsafe.Sizeof(b))
}
