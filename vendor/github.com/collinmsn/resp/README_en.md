> [中文文档](<README.md>)

# RESP

a simple and high-performance library for parsing and encoding redis protocal and redis inline command

* Repo: https://github.com/walu/resp

## Parse/Encode Redis command

there are two form of redis command, inline command and redis bulk string.

read a command from io.Reader

```go
//ex: parse command
body := []byte("setex name 10 walu\r\n") //Trailing "\r\n" can be omitted
r := bytes.NewReader(body)

cmd, err := ReadCommand(r)

//so:
//cmd.Name() == "setex"
//cmd.Value(1) == "name"
//cmd.Integer(2) == 10 (int64)
//cmd.Value(3)  == "walu"

//----------------------------
//encode command
cmd := NewCommand("setex", "name", "10", "walu")
body := cmd.Format()
fmt.Println(body)
```

## Parse/Encode Redis Data

redis protocal has five kinds of data: SimpleString、Error、BulkString、Interger、Array

resp use a struct named Data representing the five kinds of data.

```go
type Data struct {
	T byte 		//the type
	String []byte	//valid when type is SimpleString、Error or BulkString
	Integer int64	//valid when type is Interger
	Array []*Data	//valid when type is Array
	IsNil bool	//
}
```

Read a Data from io.Reader

```
//ex: parse command
body := []byte("+I'm simple string\r\n")
r := bytes.NewReader(body)

data, err := ReadCommand(r)
```
