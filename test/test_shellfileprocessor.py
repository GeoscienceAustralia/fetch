from fetch._core import ShellFileProcessor
import os

def test_shellfilepro():
    command = 'ls'
    expect_file = None
    required_files = ('(.*.hdf)',['{group0}','{group0}.xml'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, required_files=required_files)

    print (file_path)
    sfp.process(file_path)

    # Use this great function
    # RegexpOutputPathTransform


if __name__ == '__main__':
    test_shellfilepro()