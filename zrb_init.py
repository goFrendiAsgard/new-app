from zrb import (
    runner, CmdTask, EnvFile, HTTPChecker, PortChecker
)

start_docker = CmdTask(
    name='start-docker',
    cmd=[
        'docker compose down',
        'docker compose up',
    ],
    checkers=[
        PortChecker(port=5672, timeout=5),
        HTTPChecker(port=15672, method='GET'),
        PortChecker(port=9092, timeout=5),
    ]
)
runner.register(start_docker)

start = CmdTask(
    name='start',
    upstreams=[start_docker],
    cmd=[
        'if [ -f .env ]',
        'then',
        '  source .env',
        'fi',
        'cd src',
        'python main.py'
    ],
    env_files=[
        EnvFile(env_file='src/template.env')
    ],
    checkers=[
        HTTPChecker(port=8080, method='GET')
    ]
)
runner.register(start)

