import gzip
import os


outputDir = 'users_info'

def main():
    for e in os.scandir(outputDir):
        if e.name.endswith('.json'):
            print(f"Compressing: {e.name}")
            with gzip.open(f"{e.path}.gz", 'wt') as fo, open(e.path) as fi:
                fo.write(fi.read())
            os.remove(e.path)

if __name__ == '__main__':
    main()
