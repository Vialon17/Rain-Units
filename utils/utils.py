import time, json, base64

from functools import wraps
from typing import Literal

import qrcode
from PIL import Image, ImageDraw, ImageFont
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from Crypto.Random import get_random_bytes


def timer(func: object):
    @wraps(func)
    def inner(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"{func.__name__} took {elapsed_time:.4f} seconds to execute.")
        return result

    return inner


def piece_response(code: int, info: str) -> dict:
    if isinstance(code, str):
        code = int(code)
    info_key = "res" if code == 20003 else "data"
    return json.dumps({"code": code, info_key: info}, ensure_ascii=False)


def generate_qrcode(
    qr_info: str,
    save_path: str = None,
    add_info: str | list = None,
    info_gap: int = 25,
    info_border: int = 5,
    text_position: Literal["left", "right", "top", "bottom"] = "right",
    text_direction: Literal["h", "v"] = "h",
    font_style: dict = {
        "font": "msyh.ttc",
        "size": 16,
    },
) -> bool | Image.Image:
    """
    Generate QrCode With Text Based On PIL & [qrcode](https://pypi.org/project/qrcode/)

    `qr_info` -> the QRCode text info, like url

    `save_path` -> if not None, save the image to this file, else return the PIL.Image object

    `add_info` -> the text list that need show in beside QrCode

    `info_gap` -> text spacing

    `text_position` -> assigned text shown position

    `text_direction` -> assigned text shown direction

    `font_style` -> font style dict

    Other Info Please Reference [PIL Doc](https://pillow.readthedocs.io/en/stable/).
    """

    def calc_text_size(word_width, word_height) -> tuple:
        assert text_direction in (
            "h",
            "v",
        ), f"ValueError, Get Wrong Direction {text_direction}"
        max_line_length = max(len(line) for line in add_info)

        if text_direction == "h":
            text_width = max_line_length * word_width
            text_height = len(add_info) * word_height + (len(add_info) - 1) * info_gap
        else:
            text_width = len(add_info) * word_width + (len(add_info) - 1) * info_gap
            text_height = max_line_length * word_height
        return text_width, text_height

    def calc_gene_size(text_size, qr_size) -> tuple:
        gene_width, gene_height = 0, 0
        match text_position:
            case "left" | "right":
                gene_width = qr_size[0] + text_size[0] + info_border
                gene_height = max(qr_size[1], text_size[1])
            case "top" | "bottom":
                gene_width = max(qr_size[0], text_size[0])
                gene_height = qr_size[1] + text_size[1] + info_border
        return gene_width, gene_height

    def calc_text_loc(word_count: int) -> tuple:
        match text_position:
            case "left":
                text_location = (
                    (index + 1) * info_gap,
                    (gene_height - word_height * word_count) / 2,
                )
            case "right":
                text_location = (
                    qr_image.size[0] + index * info_gap,
                    (gene_height - word_height * word_count) / 2,
                )
            case "top":
                text_location = (
                    (gene_width - word_width * word_count) / 2,
                    (index + 1) * info_gap,
                )
            case "bottom":
                text_location = (
                    (gene_width - word_width * word_count) / 2,
                    qr_image.size[1] + index * info_gap,
                )
        return text_location

    # create basic qrcode
    qr = qrcode.QRCode(
        version=4,
    )
    qr.add_data(
        qr_info,
    )
    qr.make(fit=True)
    qr_image = qr.make_image(fill_color="black", back_color="white")

    if add_info:
        # initialize configuration
        font = ImageFont.truetype(**font_style)

        # create generated image
        _, _, word_width, word_height = font.getbbox(add_info[0][0])
        text_size = calc_text_size(word_width, word_height)
        gene_width, gene_height = calc_gene_size(text_size, qr_image.size)
        gene_image = Image.new("RGB", (gene_width, gene_height), "white")

        # paste qrcode image to the new generated image to suitable position
        match text_position:
            case "left":
                gene_image.paste(qr_image, (text_size[0], 0))
            case "right":
                gene_image.paste(qr_image, (0, 0))
            case "top":
                gene_image.paste(
                    qr_image, ((gene_width - qr_image.size[0]) // 2, text_size[1])
                )
            case "bottom":
                gene_image.paste(qr_image, ((gene_width - qr_image.size[0]) // 2, 0))

        # draw info text to the image
        drawer = ImageDraw.Draw(gene_image)
        for index, tiny in enumerate(add_info, 0):
            need_cut = tiny.isnumeric() and text_position not in ("left", "right")
            word_count = len(tiny) / 2 if need_cut else len(tiny)
            format_tiny = tiny if text_direction == "h" else "\n".join(tiny)
            text_location = calc_text_loc(word_count)
            drawer.text(
                text_location, format_tiny, font=font, fill="black", align="center"
            )
    else:
        # generate basic qrcode
        gene_image = Image.new("RGB", qr_image.size, "white")
        gene_image.paste(qr_image, (0, 0))

    gene_image.show("temp")
    if save_path:
        gene_image.save(save_path)
        return True
    else:
        return gene_image


def aes_encrypt(plain_text: str, key: str):
    """
    Use Assigned AES Key Encrypt Text
    """
    iv = get_random_bytes(AES.block_size)

    cipher = AES.new(key, AES.MODE_CBC, iv)

    encrypted_text = cipher.encrypt(pad(plain_text.encode("utf-8"), AES.block_size))
    return base64.b64encode(iv + encrypted_text).decode("utf-8")


def aes_decrypt(encrypted_text: str, key: str):
    """
    Decode AES Encrypted Text With Assigned Key
    """
    encrypted_data = base64.b64decode(encrypted_text)

    iv = encrypted_data[: AES.block_size]
    encrypted_text = encrypted_data[AES.block_size :]

    cipher = AES.new(key, AES.MODE_CBC, iv)

    decrypted_text = unpad(cipher.decrypt(encrypted_text), AES.block_size)
    return decrypted_text.decode("utf-8")


class Dot_Dict(dict):
    """
    Create A Packaged Dict Object That Allow `dot .`

    >>> Original_Dict = {'a':'123', 'b': 1}
    >>> Original_Dict['a']
    123
    >>> Original_Dict.a
    Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
    AttributeError: 'dict' object has no attribute 'a'
    >>> Packaged_Dict = Const(Original_Dict)
    >>> Packaged_Dict.a
    123
    >>> Packaged_Dict.A
    123
    >>> Packaged_Dict.abc
    None
    """

    def __init__(self, data: dict = None) -> None:
        super().__init__(self)
        if data is None:
            data = dict()

        for key, value in data.items():
            key = key.title()
            if isinstance(value, dict):
                # Convert sub-dictionaries to Const objects
                setattr(self, key, Dot_Dict(value))
            else:
                setattr(self, key, value)

    def __repr__(self):
        return str(self.__dict__)

    def __getattr__(self, name: str):
        # This method is only called if the attribute wasn't found the usual ways
        name = name.title()
        return self.__dict__.get(name, None)

    def __getitem__(self, name: str):
        name = name.title()
        return self.__dict__.get(name, None)

    def __setattr__(self, name: str, value):
        # Set attribute value
        name = name.title()
        self.__dict__[name] = value

    def __setitem__(self, name: str, value):
        name = name.title()
        self.__dict__[name] = value

    def update(self, key: str, value: dict | str) -> bool:
        *wrapper, key = key.split(".")
        father = None
        for father_key in wrapper:
            father = self.__dict__[father_key.title()]
        if father is not None:
            father[key.title()] = value
        else:
            self.__dict__[key.title()] = value
        return True


if __name__ == "__main__":
    # generate_qrcode(
    #     'http://oa.nsyy.com.cn:6060/?id=l1',
    #     './temp.png',
    #     ['南石医院地点码', '124514'],
    #     text_direction = 'v', text_position = 'left')

    # key = get_random_bytes(16)
    # text = '测试123'
    # aes_text = aes_encrypt(text, key)
    # print(aes_text)
    # print(aes_decrypt(aes_text, key))

    test_dict = Dot_Dict(
        {
            "temp": {
                "home": "unknown",
                "age": 12,
                "number": 5,
                "mates": ["job", "work", "dialy"],
                "temp3": {"a": "321312", "b": [1, 4, 5]},
            },
            "temp2": 123,
        }
    )
    print(test_dict.temp.temp3.b)
