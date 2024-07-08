from dataclasses import dataclass
from dataclasses import field
import os
import subprocess
import sys

import bm
from bm.iast_utils.ast_patching import create_project_structure
from bm.iast_utils.ast_patching import destroy_project_structure


@dataclass
class IAST_AST_PatchingParent:
    name: str
    iast_enabled: bool = field(default_factory=bm.var_bool)


class IAST_AST_Patching(IAST_AST_PatchingParent, bm.Scenario):
    def run(self):
        try:
            python_file_path = create_project_structure()

            env = os.environ.copy()
            env["DD_IAST_ENABLED"] = str(self.iast_enabled)

            subp_cmd = ["ddtrace-run", sys.executable, python_file_path]

            def _(loops):
                for _ in range(loops):
                    subprocess.check_output(subp_cmd, env=env)

            yield _

        finally:
            destroy_project_structure()
