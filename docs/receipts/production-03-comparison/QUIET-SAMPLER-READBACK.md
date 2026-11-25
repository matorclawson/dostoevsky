# Quiet sampler readback

The initial sampler recorded timestamped load readings every five seconds. Its decision check failed because GNU awk reserves the variable name `load`. The failed launcher output is retained in `RUN-TRANSPORT.log`.

The corrected launcher checked the recorded samples for gaps longer than six seconds, verified that the last sample was current and rechecked the live load. That readback established 371 seconds below 0.30 before the window opened. No smoke or timed trial started from the initial launcher. The sample log and corrected launch scripts are in this receipt.
