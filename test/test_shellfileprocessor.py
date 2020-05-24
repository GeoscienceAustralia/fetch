
import os
from fetch._core import RegexpOutputPathTransform
from fetch._core import ShellFileProcessor

def test_shellfilepro_required_files_there():
    command = 'ls {base}.py'
    expect_file = None
    required_files = (r'^(?P<base>.*test.+)\.py$',['{base}.py','{base}.py'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, required_files=required_files)

    sfp.process(file_path)

    #t = RegexpOutputPathTransform(r'^(?P<base>.*test.+)\.py$')
    #print(t.transform_output_path('{base}.py', '/yeah/test_shell.py'))



def test_shellfilepro_required_files_not_there():
    command = 'ls {base}.py'
    expect_file = None
    required_files = (r'^(?P<base>.*test.+)\.py$',['{base}.py','/this/is/not/here/please.py'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, required_files=required_files)

    sfp.process(file_path)
if __name__ == '__main__':
    test_shellfilepro()