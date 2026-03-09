Output doc/screenshots/repl-engine.gif

Set FontSize 14
Set Width 900
Set Height 700
Set Theme "Catppuccin Mocha"
Env PATH "/home/james/gits/database/.bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

Type "db demo-preloaded.db"
Enter
Sleep 1500ms

Type "enter engine"
Enter
Sleep 400ms

Type "compile SELECT id, name FROM users WHERE age > 27 ORDER BY name"
Enter
Sleep 500ms

Type "program"
Enter
Sleep 4s
