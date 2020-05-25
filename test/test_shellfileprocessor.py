
import os
from fetch._core import ShellFileProcessor


def test_shellfilepro_required_files_there():
    command = 'ls {base}.py'
    # required_files = (r'^(?P<base>.*test.+)\.py$',['{base}.py','{base}.py'])
    required_files = ('^(?P<base>.*test.+)\\.py$', ['{base}.py', '{base}.py'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, required_files=required_files)
    results = sfp.process(file_path)

def test_shellfilepro_required_files_not_there():
    command = 'ls {base}.py'
    required_files = (r'^(?P<base>.*test.+)\.py$', ['{base}.py', '/this/is/not/here/please.py'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, required_files=required_files)
    results = sfp.process(file_path)
    # A rather toothless assert given required_files_there would return the same result...
    assert file_path == results
