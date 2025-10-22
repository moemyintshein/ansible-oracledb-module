#!/usr/bin/python

from ansible.module_utils.basic import AnsibleModule

try:
    import oracledb
except ImportError:
    oracledb = None


def run_module():
    module_args = dict(
        user=dict(type='str', required=True),
        password=dict(type='str', required=True, no_log=True),
        dsn=dict(type='str', required=True),   # e.g. "host:1521/service_name"
        query=dict(type='str', required=True),
        params=dict(type='list', elements='str', required=False, default=[]),
        commit=dict(type='bool', default=False)
    )

    result = dict(
        changed=False,
        rows=[]
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    if oracledb is None:
        module.fail_json(msg="Python oracledb library is required (pip install oracledb)")

    try:
        # Connect (thin mode by default)
        conn = oracledb.connect(
            user=module.params['user'],
            password=module.params['password'],
            dsn=module.params['dsn']
        )
        cur = conn.cursor()
        cur.execute(module.params['query'], module.params['params'])

        # If it's a SELECT, fetch results
        if module.params['query'].strip().lower().startswith("select"):
            result['rows'] = cur.fetchall()

        # Commit only if requested
        if module.params['commit']:
            conn.commit()

        cur.close()
        conn.close()

    except Exception as e:
        module.fail_json(msg=f"Oracle query failed: {str(e)}")

    module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
