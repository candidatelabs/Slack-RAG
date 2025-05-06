tell application "Terminal"
    activate
    do script "cd /Users/david/Desktop/slack-digest-experimental && ./run_slack_digest.sh"
end tell

delay 2

tell application "Google Chrome"
    activate
    open location "http://127.0.0.1:5001"
end tell 