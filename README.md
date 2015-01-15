# raintank-collector-ctrl

The raintank-collector is a Socket.io service for distributing monitoring checks to raintank-collectors.  The service is also the channel through which the raintank-collectors submit the metrics they have collected.

## building and running

Clone the repository
```
git clone <this_repo>
```

Install all of the dependent node_modules

```
npm install
```

Update the config.js file with configuration options.

Then start the app.

```
nodejs app.js
```
