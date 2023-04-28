import os
from flask import Flask, request, Response, abort
from typing import List, Optional, Union, Iterator, Generator

app: Flask = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def _limit(data: str, value: int) -> Generator:
    """ Функция для вывода данных в ограниченном размере """
    i: int = 0
    for k in data:
        if i > int(value):
            break
        yield k
        i += 1


def _command(data: Union[str, list, Iterator, Generator], cmd: str, value: str) -> Union[Iterator, Generator, None]:
    """ Функция, выполняющая команды """
    if cmd == "map":
        return map(lambda x: x.split(" ")[int(value)], data)
    if cmd == "filter":
        return filter(lambda x: value in x, data)
    if cmd == "unique":
        return iter(set(data))
    if cmd == "sort":
        return sorted(data, reverse=False)
    if cmd == "limit":
        return _limit(data, int(value))
    return None


def query(data, cmd1: str, cmd2: str, value1: str, value2: str) -> List[Union[str, Iterator, Generator]]:
    data1: Union[Iterator, Generator, None] = _command(data, cmd1, value1)
    data2: Union[Iterator, Generator, None] = _command(data, cmd2, value2)
    return [data1, data2]


@app.route("/")
@app.route("/perform_query/")
def perform_query() -> Response:
    data: request = request.json
    if data is False:
        abort(404, 'Exception')

    cmd1: Optional[str] = data.get('cmd1')
    cmd2: Optional[str] = data.get('cmd2')
    value1: Optional[str] = data.get('value1')
    value2: Optional[str] = data.get('value2')
    filename: Optional[str] = data.get('filename')

    file_path: str = os.path.join(DATA_DIR, filename)

    if os.path.exists(file_path):
        with open(file_path) as f:
            result: Union[str, Iterator, Generator] = '\n'.join(query(f, cmd1, cmd2, value1, value2))
    else:
        abort(404, "Нет файла")

    return app.response_class(result, content_type="text/plain")


if __name__ == '__main__':
    app.run(debug=True, host="localhost")
