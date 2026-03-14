Output doc/screenshots/repl-debug.gif
Output doc/screenshots/repl-debug.txt

Set FontSize 14
Set Width 1300
Set Height 800
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Wait
Type "rm test.db"
Enter
Wait
Sleep 500ms

Type "db"
Sleep 1s
Type " test "
Backspace
Type ".db"
Sleep 3s
Type " "
Sleep 2s
Type "file"
Sleep 500ms
Type " "
Sleep 500ms
Type "tests/"
Sleep 1s
Type "sq"
Tab
Sleep 5s
Type "/inn"
Tab
Sleep 500ms
Enter
Sleep 2.5s

Type "db  test.db"
Enter
Wait
Sleep 2.5s

Type "enter engine"
Enter
Wait
Sleep 2s

Type "compile SELECT id, name FROM employees WHERE dept_id = 1"
Enter
Wait
Sleep 500ms

Type "debug"
Enter
Sleep 1s
Type@100ms "                                                                  "
Sleep 500ms
Type "rrrr"
Sleep 2.5s
Type "R"
Sleep 1.5s
Type@40ms "                                                                                                                                                                                                                                                      "
Sleep 500ms
Type "q"
Sleep 1s
Ctrl+D
Sleep 500ms
Ctrl+D
