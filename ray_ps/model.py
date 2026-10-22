"""Математическая модель, общая для Ray-приложения и последовательного эталона.

ConvNet адаптирован из примера Ray: см. THIRD_PARTY.md и LICENSES/Ray.txt.
Код координации и восстановления здесь намеренно отсутствует.
"""

import hashlib

import torch
from torch import nn
from torch.nn import functional as F


def configure_torch(seed):
    torch.set_num_threads(1)
    torch.manual_seed(seed)
    torch.use_deterministic_algorithms(True)


class ConvNet(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(1, 3, kernel_size=3)
        self.fc = nn.Linear(192, 10)

    def forward(self, images):
        features = F.relu(F.max_pool2d(self.conv1(images), 3))
        return self.fc(features.flatten(start_dim=1))


def copy_weights(model):
    return {name: value.detach().cpu().clone() for name, value in model.state_dict().items()}


def momentum_buffers(model, optimizer):
    return {
        name: optimizer.state[parameter]["momentum_buffer"].detach().cpu().clone()
        for name, parameter in model.named_parameters()
        if "momentum_buffer" in optimizer.state[parameter]
    }


def tensor_digest(tensors):
    """Hash canonical tensor data, not torch.save's archive metadata."""
    digest = hashlib.sha256()
    for name, tensor in sorted(tensors.items()):
        array = tensor.detach().cpu().contiguous().numpy()
        digest.update(f"{name}:{array.dtype}:{array.shape}\n".encode())
        digest.update(array.tobytes())
    return digest.hexdigest()


def images_and_labels(dataset, indices):
    images = dataset.data[indices].unsqueeze(1).float().div(255)
    return (images - 0.1307) / 0.3081, dataset.targets[indices]
