# Query Abstraction Layer


QAL is a collection of libraries for mining, transforming and writing data from and to a number of places. 

Sources and destinations include different SQL and NoSQL backends, file formats like .csv, XML and excel. Even untidy HTML web pages. 
It has a database abstraction layer that supports connectivity to Postgres, MySQL, DB2, Oracle, MS SQL server and SQLite. 
It uses JSON formats(self-generated JSON schemas) for representing queries, transformation and merging, making it scriptable.

This means that QAL can be backend agnostic about a subset of SQL features and data types. Of course custom SQL:s are also supported.

More general information at: http://www.optimalbpm.se/wiki/index.php/QAL
It is currently most conveniently distributed as a Python 3 Library (pip3 install python3-qal) and Debian .deb package.

It is related the Optimal BPM project, see its [repository for more information](https://github.com/OptimalBPM/optimalbpm)


# Installation intructions


## Using pip

Notice that there are several dependencies that may needs satisfying on some platforms, like database drivers.

```bash
pip3 install python3-qal
```

## Ubuntu/Debian


### From launchpad.net PPA in Ubuntu:


Execute the following commands:
```bash
sudo apt-add-repository ppa:nicklasb/qal
sudo apt-get update
sudo apt-get install python3-qal
```

### From launchpad.net PPA in Debian(Jessie and onward):

Add the following line to /etc/apt/sources.list, the wildcard is there as the there are no matching distro names in
launchpad.net, this way it takes what it gets. It is the same package anyway.

First, using a privileged user, add this to /etc/apt/sources.list
```bash
deb http://ppa.launchpad.net/nicklasb/qal/ubuntu * main
```
Then execute these commands:
```bash
apt-get update
apt-get install python3-qal
```

Notes for Debian:

1. the root user can be replaced by any other sufficiently privileged user
2. there is a non-standard unicode character in the maintainer's name, this *might* cause problems if you have an old locale.

```bash
LANG=C.UTF-8
```
...might help, read this first though: http://askubuntu.com/questions/393638/unicodedecodeerror-ascii-codec-cant-decode-byte-0x-in-position-ordinal-n


## Without* launchpad.net PPA in either distribution:


gdebi is recommended*, to install gdebi, use:
```bash
 sudo apt-get install gdebi
```


Install package, available for download above:
```bash
sudo gdebi python3-qal_0.4.0_all.deb
```

## On a tight system
There gdebi can be considered a bit bulky. However, QAL itself is among other things a Python 3-based data mining library and therefore(currently at least) requires a lot of libraries. If you have severe constraints it is perhaps not the optimal solution.


##  Windows

Installation instructions for Windows:
The pip tool is included in Python 3.4 and forwards.
```bash
pip3 install python3-qal
```

# Development

Unit tests are not included in the .deb-package, for that, install the .egg:
```bash
pip3 install python3-qal
```
or clone the source:
```bash
git clone git://git.code.sf.net/p/qal/code qal-code
```


# License

Copyright (c) 2016 Optimal BPM

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.