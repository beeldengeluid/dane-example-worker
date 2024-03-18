# dane-example-worker

## Locally test the worker

To locally test the worker, first navigate to the directory where the repository is located, then run this command:

```
docker run --mount type=bind,source="$(pwd)"config,target=/root/.DANE --mount type=bind,source="$(pwd)"data,target=/src/data --rm workshop --run-test-file
```

Explanation:

- `--mount` is responsible for adding the files required for testing. It essentially copies the files from your local directory to a target directory within the container
- `"$(pwd)"` outputs the current directory you are in. This is required because Docker uses the absolute paths when copying files to/from a container
- The first `--mount` copies the `config` folder, whereas the second one copies the input file
- It is important to have the `--mount` before calling the actual image, otherwise `--mount` will be treated as a flag of the `worker.py` script to be executed
