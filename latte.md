
1. Q&A
mac M1系统安装了llvm 后引入 $LDFLAGS=-L/opt/homebrew/opt/llvm/lib 导致报错 flag provided but not defined: -L/opt/homebrew/opt/llvm/lib
解决方案
```shell 
export LDFLAGS=""
make
```
