# Scala


## 变量
1. var VariableName : DataType [=  Initial Value]
2. val VariableName : DataType [=  Initial Value]
3. public private protected
4. String不可变，可以使用StringBuilder和"+="，使用Str.length()返回长度
5. 数组：var z = new Array[String](3)    var z = Array("Runoob", "Baidu", "Google")
6. for循环：
   - for ( x <- myList ) { println( x ) }
   - for ( i <- 0 to (myList.length - 1)) { total += myList(i); }


## 方法
1. 抽象方法：def functionName ([参数列表]) : [return type]
2. 方法定义：def functionName ([参数列表]) : [return type] = { function body }
3. 如果方法没有返回值，可以返回为 Unit，这个类似于 Java 的 void, def printMe( ) : Unit = { }


## 集合
1. 定义整型 List：val x = List(1,2,3,4)
2. 定义 Set：val x = Set(1,3,5,7)
3. 定义 Map：val x = Map("one" -> 1, "two" -> 2, "three" -> 3)
4. 创建两个不同类型元素的元组：val x = (10, "Runoob")
5. 定义 Option，表示有可能包含值的容器，也可能不包含值：val x:Option[Int] = Some(5)
6. 迭代器
   - Scala Iterator（迭代器）不是一个集合，它是一种用于访问集合的方法
   - it.next()、it.hasNext()、it.max、it.min、it.size(it.length)、


## 类
1. class、new、extends
2. Object（单例对象？？？）
3. trait（相当于接口/抽象类）

