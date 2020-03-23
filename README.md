# metro

Keeping track of where Los Angeles Metro's buses have been!


Using [AWS's Serverless Application Model (SAM) framework](https://aws.amazon.com/serverless/sam/), `metro` collects vehicle ping data from [api.metro.net](https://api.metro.net) - assembling them into historical records (_paths_) saved in a database -  and exposes an API that delivers GeoJSON paths queryable by route ID and time.

## Visualization

![Animated map of LA Metro Buses](https://vertuli-public.s3-us-west-2.amazonaws.com/metro_kepler.gif)

The output GeoJSON can be saved as a file and fed directly into [kepler.gl](http://kepler.gl/demo) for animated path visualization.
