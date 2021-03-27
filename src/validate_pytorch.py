import torch

try:
    x = torch.rand(5, 3)
    print(x)
except Exception as e:
    print(e)
    print("Pytorch is not installed")


try:
    if not torch.cuda.is_available():
        print("Torch CUDA is not available.")
except Exception as e:
    print(e)
    print("Torch CUDA is not available.")
