var JLogger = require('/usr/local/share/jam/lib/jserver/jlogger');
var JManager = require('/usr/local/share/jam/lib/jserver/jmanager');

jdata{
    int num_bugs as logger;
}

setTimeout(
    function(){
        console.log(num_bugs.jlogger_get_newest_value());
    }, 15000);