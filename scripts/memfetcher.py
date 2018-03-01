from psutil import virtual_memory

mem = virtual_memory()
print(round(mem.total/(1024.**3)))

