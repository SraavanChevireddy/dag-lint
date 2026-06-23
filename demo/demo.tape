# VHS tape for dag-lint demo. Render with:  vhs demo/demo.tape
Output demo/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1200
Set Height 700
Set Theme "Catppuccin Mocha"
Set Padding 20
Set TypingSpeed 45ms
Set LoopOffset 40%

Hide
Type "export PATH=/tmp/demoenv/bin:$PATH && clear" Enter
Show

Type "# dag-lint: catch Airflow DAG anti-patterns before they ship" Sleep 1s Enter
Type "cat examples/bad_dag.py" Sleep 500ms Enter
Sleep 2.5s

Type "dag-lint examples/bad_dag.py" Sleep 500ms Enter
Sleep 4s

Type "# fix the DAG, and it passes clean (great for CI)" Sleep 1s Enter
Sleep 2s
