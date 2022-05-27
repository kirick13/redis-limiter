
const REDIS_PREFIX = '@limiter:';

class RedisLimiter {
    constructor (redisClient, name, limits) {
        this._redisClient = redisClient;

        this._redis_scripts = {
            hit: redisClient.createScript(`
                local name = ARGV[1]
                local key = ARGV[2]
                local element = ARGV[3]

                for i = 4, #ARGV, 4 do
                    local limit_name = ARGV[i]
                    local limit_value = tonumber(ARGV[i + 1])

                    local redis_key = "${REDIS_PREFIX}" .. name .. ":" .. key .. ":" .. limit_name

                    local value = redis.pcall(
                        "GET",
                        redis_key
                    )
                    if "x" == value then
                        return {
                            limit_name,
                            redis.call(
                                "TTL",
                                redis_key
                            )
                        }
                    end

                    local value_new = 0
                    if "" == element then
                        value_new = redis.call(
                            "INCR",
                            redis_key
                        )
                    else
                        local added = redis.call(
                            "SADD",
                            redis_key,
                            element
                        )
                        if 0 == added then
                            return {}
                        end
                        value_new = redis.call(
                            "SCARD",
                            redis_key
                        )
                    end

                    if value_new > limit_value then
                        redis.call(
                            "SET",
                            redis_key,
                            "x",
                            "KEEPTTL"
                        )

                        local ttl_block = tonumber(ARGV[i + 3])
                        if ttl_block > 0 then
                            redis.call(
                                "EXPIRE",
                                redis_key,
                                ttl_block
                            )
                            return {
                                limit_name,
                                ttl_block
                            }
                        else
                            return {
                                limit_name,
                                redis.call(
                                    "TTL",
                                    redis_key
                                )
                            }
                        end
                    elseif 1 == value_new then
                        redis.call(
                            "EXPIRE",
                            redis_key,
                            tonumber(ARGV[i + 2])
                        )
                    end
                end

                return {}
            `),
        };

        this._name = name;

        this._limit_names = [];
        this._redis_limit_args = [];
        this._error_listeners = new Map();
        for (const [
            limit_name,
            {
                limit,
                ttl,
                ttl_block,
                onError,
            },
        ] of Object.entries(limits)) {
            this._limit_names.push(limit_name);

            this._redis_limit_args.push(
                limit_name,
                limit,
                ttl,
                ttl_block ?? 0,
            );

            this._error_listeners.set(
                limit_name,
                onError,
            );
        }
    }

    async check (key) {
        const multi = this._redisClient.MULTI();

        for (const limit_name of this._limit_names) {
            const redis_key = this._name + ':' + key + ':' + limit_name;

            multi.GET(redis_key);
            multi.TTL(redis_key);
        }

        const result = await multi.EXEC();

        for (let ix = 0; ix < result.length; ix += 2) {
            const value = result[ix];

            if ('x' === value) {
                const limit_name = this._limit_names[ix / 2];
                const ttl = result[ix + 1];

                this._error_listeners.get(limit_name).call(this, ttl);

                const error = new Error('Limit exceeded');
                error.code = 'LIMIT_EXCEEDED';
                error.package = '@kirick/redis-limiter';
                throw error;
            }
        }
    }

    async hit (key, element) {
        const [ limit_name, ttl ] = await this._redis_scripts.hit.run(
            this._name,
            key,
            element ?? '',
            ...this._redis_limit_args,
        );

        if (typeof limit_name === 'string') {
            this._error_listeners.get(limit_name).call(this, ttl);

            const error = new Error('Limit exceeded');
            error.code = 'LIMIT_EXCEEDED';
            error.package = '@kirick/redis-limiter';
            throw error;
        }
    }

    async reset (key, limit_names) {
        const keys_to_delete = new Set();

        for (const limit_name of (limit_names ?? this._error_listeners)) {
            keys_to_delete.add(
                REDIS_PREFIX + this._name + ':' + key + ':' + limit_name,
            );
        }

        if (keys_to_delete.size > 0) {
            await this._redisClient.DEL(
                ...keys_to_delete,
            );
        }
    }
}

module.exports = RedisLimiter;
