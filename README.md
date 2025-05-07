# delta-lake-test-run


## Build container image

```bash
podman build -t delta-lake-local .
podman run -it -v $(pwd):/app delta-lake-local bash

python3 example_usage.py
```