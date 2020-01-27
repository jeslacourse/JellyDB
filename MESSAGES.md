# Messages

## Lisa says:
Let's keep the README as a standard README and put internal messages here.

Correct me if I'm wrong on this because I'm not a CS person... I read that a standard project structure is to put the `gitignore`, `README`, `docs/` folder, etc. in the top-level folder (whose name is the name of the package), and put all the source code in a subfolder whose name is also the name of the package.

So I made a copy of the `template` directory, named it `JellyDB`, and replaced `template` with `JellyDB` in all the import statements.

I got it to run in python 3.6.0 like this:
```
$ ls
JellyDB		MESSAGES.md	README.md	template
$ python -m JellyDB
Inserting 10k records took:  			 0.0068969999999999865
Updating 10k records took:  			 0.03211299999999999
Selecting 10k records took:  			 0.019483000000000014
Aggregate 10k of 100 record batch took:	 0.00034300000000000996
Deleting 10k records took:  			 0.0031930000000000014
```
