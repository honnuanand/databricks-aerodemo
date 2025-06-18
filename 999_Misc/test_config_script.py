from conf.config_reader import (
    load_env_configs,
    upsert_config,
    upsert_multiple_configs,
    delete_env_configs
)


def main():
    # ---------- CONFIG ----------
    env = "dev"
    print(f"\n🚀 Starting config test for environment: {env}")

    try:
        # 🔍 Load configs
        print(f"\n🔍 Loading configs...")
        configs = load_env_configs(env)
        print(f"✅ Loaded configs: {configs}")

        # 🔧 Upsert a single test config
        print(f"\n🔧 Upserting a single test config...")
        upsert_config(env, "test_key", "test_value")
        print("✅ Single upsert complete.")

        # 🔧 Upsert multiple configs
        print(f"\n🔧 Upserting multiple configs...")
        batch_configs = {
            "batch_key_1": "batch_value_1",
            "batch_key_2": "batch_value_2"
        }
        upsert_multiple_configs(env, batch_configs)
        print("✅ Batch upsert complete.")

        # 🔍 Verify updated configs
        configs = load_env_configs(env)
        print(f"\n✅ Configs after upserts: {configs}")

        # ⚠️ Delete all configs for the environment
        print(f"\n⚠️ Deleting all configs for '{env}'...")
        delete_env_configs(env)
        print("✅ Deletion complete.")

        # 🔍 Verify deletion
        configs = load_env_configs(env)
        print(f"\n✅ Final configs after deletion (should be empty): {configs}")

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise


if __name__ == "__main__":
    main()