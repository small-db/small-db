import json
import subprocess

package = "small_tests"
commond = f"cargo test --package small-db --test {package} --all-features --no-run --message-format=json"
result = subprocess.run(commond.split(), stdout=subprocess.PIPE)
s = result.stdout.decode('utf-8')
lines = s.splitlines()
for line in lines:
    obj = json.loads(line)
    if "executable" in obj:
        v = obj["executable"]
        if v:
            if package in v:
                print(v)
                break
