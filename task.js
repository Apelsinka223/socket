
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

// Запуск для вывода накопившихся сообщений об ошибке
if (process.argv[2] == 'getErrors')
{
    client.smembers('error_messages', function(err, res) {
        if (err)
        {
            logger.error(err);
            client.end();
        }
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
    // Задаем pid процессу (pid последнего запущенного процесса lastID + 1)
    // и записываем новый lastID
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

    // Если не задан сервер - то данный процесс будет сервером,
    // иначе - слушателем
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

// Генерация сообщений (из задания)
function getMessage(){
    this.cnt = this.cnt || 0;
    return this.cnt++;
}

// Обработка сообщений (из задания)
function eventHandler(msg, callback){
    function onComplete(){
        var error = Math.random() > 0.85;
        callback(error, msg);
    }
    // processing takes time...
    setTimeout(onComplete, Math.floor(Math.random()*1000));
}

// Запуск сервера
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
    // Генерируем сообщения
    var generator = setInterval(function () {
        // Запоминаем сервер на 1 секунду
        // Если сервер упадет, то запись очистится и выберется новый сервер
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
     //if (message >= 1000000)
    //{
        //    clearInterval(generator);
        //    client.persist('server');
        //}
    //}, 0); // Для проверки что генерируется 1000000 сообщений
}

// Запуск слушателя
function listener()
{
    var message = '';
    // Слушаем сообщения
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

    }, 600); // > 500 чтобы видеть обработку несколькими приложениями
    //}, 1); // Для проверки, что генерирутся 1000000 сообщений

    // Проверяем не упал ли сервер каждую секунду
    // Если упал - делаем из процесса новый сервер
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
                // Перестаем слушать сообщения
                clearInterval(listen);
                // Перестаем проверять, упал ли сервер
                clearInterval(checkserver);
            }
        })
    }, 1000);
}
