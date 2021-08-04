import os


# Использование генератора позволяет не хранить все в памяти
def absoluteFilePaths(directory):
    for dirpath,_,filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))

for filepath in absoluteFilePaths("./"):
    print(filepath)
