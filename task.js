
var redis = require("redis");
var client = redis.createClient();

var log4js = require('log4js');
var logger = log4js.getLogger();

var pid = 0;

client.on("error", function (err) {
    if (err)
    {
        logger.error(err);
        client.end();
    }
});

if (process.argv[2] == 'getErrors')
{
    client.smembers('error_messages', function(err, res) {
        if (err)
        {
            logger.error(err);
            client.end();
        }
        console.log(res);
        client.del('error_messages', function(err) {
            if (err)
            {
                logger.error(err);
                client.end();
            }
        });
        client.end();
    });

}
else
{
    client.get("lastID", function (err, res) {
        if (err)
        {
            logger.error(err);
            client.end();
        }
        if (res !== null)
        {
            pid = parseInt(res) + 1;
        }
        client.set('lastID', pid, function(err) {
            if (err)
            {
                logger.error(err);
                client.end();
            }
        });

        logger.info('pid: ' + pid);
    });

    client.get("server", function(err, res) {
        if (err)
        {
            logger.error(err);
            client.end();
        }
        if (res == null)
        {
            server();
        }
        else
        {
            listener();
        }
    });
}


function getMessage(){
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

function eventHandler(msg, callback){
    function onComplete(){
        var error = Math.random() > 0.85;
        callback(error, msg);
    }
// processing takes time...
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}

function server()
{
    client.set('server', pid, function (err) {
        if (err)
        {
            logger.error(err);
            client.end();
        }
        logger.info('server: ' + pid);
    });
    var generator = setInterval(function () {
        client.expire('server', 1);
        var message = getMessage();

        client.sadd('messages', message,  function (err) {
            if (err)
            {
                logger.error(err);
                client.end();
            }
            logger.info("Published message '" + message + "'");
        });
    }, 500);
}

function listener()
{
    var message = '';
    var listen = setInterval(function () {
        client.spop('messages', function (err, res)
        {
            if (err)
            {
                logger.error(err);
                client.end();
            }
            if (res !== null)
            {
                logger.info("Got message '" + res + "'");
                eventHandler(res, function(err1, res)
                {
                    if (err1)
                    {
                        logger.info("Message '" + res + "' has error");
                        client.sadd('error_messages', res,  function (err2) {
                            if (err2)
                            {
                                logger.error(err2);
                                client.end();
                            }
                        });
                    }
                })
            }
        });

    }, 600);

    var checkserver = setInterval(function () {
        client.get('server', function(err, res)
        {
            if (err)
            {
                logger.error(err);
                client.end();
            }
            if(res == null)
            {
                logger.error("No server");
                server();
                clearInterval(listen);
                clearInterval(checkserver);
            }
        })
    }, 1000);
}
