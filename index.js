var pkgcloud = require("pkgcloud"),
    Writable = require("stream").Writable,
    _ = require("underscore");

function getClient(credentials, version) {
  if (version == 1){
    var clientInfo = {
      provider: 'openstack',
      username: credentials.userId,
      password: credentials.password,
      authUrl: credentials.auth_url,
      version: 1, 
      useServiceCatalog: false
    }
  }else{
    var clientInfo = {
      provider: 'openstack',
       username: credentials.userId,
       password: credentials.password,
       authUrl: credentials.auth_url,
       tenantId: credentials.projectId,
       region: credentials.region,
       version: "2"
    }
  }
  return pkgcloud.storage.createClient(clientInfo);
}

module.exports = function SwiftStore(globalOpts) {
    globalOpts = globalOpts || {};

    var adapter = {

        read: function(options, file, response) {
            var client = getClient(options.credentials, options.version);
            client.download({
                container: options.container,
                remote: file,
                stream: response
            });
        },

        rm: function(fd, cb) { return cb(new Error('TODO')); },

        ls: function(options, callback) {
            var client = getClient(options.credentials, options.version);

            client.getFiles(options.container, function (error, files) {
                return callback(error, files);
            });
        },

        receive: function(options) {
            var receiver = Writable({
                objectMode: true
            });

            receiver._write = function onFile(__newFile, encoding, done) {
                var client = getClient(options.credentials, options.version);
                console.log("Uploading file with name", __newFile.filename);
                __newFile.pipe(client.upload({
                    container: options.container,
                    remote: __newFile.filename
                }, function(err, value) {
                  console.log(err);
                  console.log(value);
                    if( err ) {
                      console.log( err);
                      receiver.emit( 'error', err );
                      return;
                    }
                    done();
                }));

                __newFile.on("end", function(err, value) {
                  console.log("finished uploading", __newFile.filename);
                    receiver.emit('finish', err, value );
                    done();
                });

            };

            return receiver;
        },
        ensureContainerExists: function(credentials, containerName, version, callback) {
          var client = getClient(credentials, version);

          client.getContainers(function (error, containers) {
            if (error) {
              callback(error);
              return;
            }
            if (containers.length === 0) {
              client.createContainer(containerName, callback);
            }
            else {
              var found = _.find(containers, function (container) {
                  return container.name === containerName;
              });
              if (found === undefined) {
                client.createContainer(containerName, callback);
              }
              else {
                callback(null);
              }
            }

          });
        }
    }

  return adapter;
}
