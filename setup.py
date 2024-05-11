from cx_Freeze import setup, Executable
import sys

base = None

if sys.platform == 'win32':
    base = None


executables = [Executable("automato.py", base=base)]

packages = ["idna"]
options = {
    'build_exe': {

        'packages':packages,
    },

}

setup(
    name = "Automato",
    options = options,
    version = "1.0",
    description = 'Desenvolvido pela seção de BD e Controle de qualidade para carga no Sigop',
    executables = executables
)