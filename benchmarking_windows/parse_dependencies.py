import os
import sys

begin = False
package_version = {}
with open('./Cargo.toml') as f:
    for line in f:
        if '[' == line[0]:
            begin = False
        if 'dependencies' in line:
            begin = True
            continue

        if begin:
            sep = line.find('=')
            package_version[line[:sep-1].strip()] = line[sep+2:].strip()

for dir_path in ["./libs/iceberg/", "./libs/rest/", "./libs/test_utils/"]:
    r = open(dir_path + "Cargo.toml")
    w = open(dir_path + "Cargo_n.toml", 'w')
    begin = False
    for r_line in r:
        if '[' == r_line[0]:
            begin = False
        if 'dependencies' in r_line:
            begin = True
            w.write(r_line)
            continue

        if begin:
            sep = r_line.find('=')
            package = r_line[:sep-1].strip()
            if package in package_version:
                w.writelines([f"{package} = {package_version[package]}", "\n"])
            else:
                w.write(r_line)
        else:
            w.write(r_line)
    r.close()
    w.close()
    os.remove(dir_path + "Cargo.toml")
    os.rename(dir_path + "Cargo_n.toml", dir_path + "Cargo.toml")
