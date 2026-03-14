Output doc/screenshots/repl-engine.gif
Output doc/screenshots/repl-engine.txt

Set FontSize 14
Set Width 900
Set Height 700
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Wait

Hide
Type "cp demo-preloaded.db.seed demo-preloaded.db"
Enter
Ctrl+L
Wait
Show

Type "db demo-preloaded.db"
Enter
Wait
Sleep 1500ms

Type "enter engine"
Enter
Wait
Sleep 400ms

Type "compile SELECT id, name FROM users WHERE age > 27 ORDER BY name"
Enter
Wait
Sleep 500ms

Type "program"
Enter
Wait
Sleep 4s
