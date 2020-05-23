
import os
from fetch._core import RegexpOutputPathTransform
from fetch._core import ShellFileProcessor

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
    t = RegexpOutputPathTransform('LS8_(?P<year>\\d{4})')
    print(t.transform_output_path('/tmp/out/{year}', 'LS8_2003'))

    t = RegexpOutputPathTransform(r'LS8_(?P<year>\d{4})')
    print(t.transform_output_path('/tmp/out/{year}', 'LS8_2003'))

    t = RegexpOutputPathTransform(r'^(?P<base>test.+)\.py$')
    print(t.transform_output_path('/tmp/out/{base}.pyc', 'test_shell.py'))

    t = RegexpOutputPathTransform('^(?P<base>test.+)\\.py$')
    print(t.transform_output_path('/tmp/out/{base}.pyc', 'test_shell.py'))


    t = RegexpOutputPathTransform(r'^(?P<base>.*test.+)\.py$')
    print(t.transform_output_path('{base}.py', '/yeah/test_shell.py'))

if __name__ == '__main__':
    test_shellfilepro()