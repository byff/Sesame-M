package io.github.aw1y2z.sesame.data;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import io.github.aw1y2z.sesame.util.FileUtil;
import io.github.aw1y2z.sesame.util.Log;
import io.github.aw1y2z.sesame.util.StringUtil;

/**
 * 自动切换登录账号的全局配置，统一保存在 config/config_common.json。
 *
 * 该文件不区分账号，模块 App 与支付宝注入进程共用同一份：
 * accountSwitchEnabled              是否开启自动切换登录账号
 * accountSwitchIntervalMinutes      切号等待间隔(分钟，每个账号的最短停留时长；0=按一个执行周期)
 * accountSwitchRoundIntervalMinutes 切号轮次间隔(分钟，0=按执行间隔)
 * accountSwitchAccounts             账号列表，顺序即切号顺序，循环执行
 * accountSwitchFinalAccountUid      常态保持账号(一轮走完后切入该账号，本轮跑完再等轮次间隔)
 * accountSwitchScheduleTimes        切换账号定时执行时间点，如 ["0800","2000"]
 * autoSlideCaptchaEnabled           自动处理滑动/拼图验证
 * unlockAccountLimitEnabled         解除登录账号数量限制
 *
 * 任意一项保存时都会把全部配置一起写回，避免多个字段分别落盘互相覆盖。
 */
public final class AccountSwitchConfig {

    public static final String FILE_NAME = "config_common.json";

    private static final String TAG = AccountSwitchConfig.class.getSimpleName();
    private static final int MAX_INTERVAL_MINUTES = 24 * 60;
    /** 读缓存有效期：任务与界面都会高频读取，过期后重新读盘 */
    private static final long CACHE_TTL_MILLIS = 1000L;

    private static final Object LOCK = new Object();

    private static volatile Snapshot cache;
    private static volatile long cacheModified = Long.MIN_VALUE;
    private static volatile long cacheTime;

    private AccountSwitchConfig() {
    }

    public static File getFile() {
        return new File(FileUtil.MAIN_DIRECTORY_FILE, FILE_NAME);
    }

    /** 读取配置（1 秒内复用缓存） */
    public static Snapshot read() {
        File file = getFile();
        long modified = file.exists() ? file.lastModified() : -1;
        long now = System.currentTimeMillis();
        Snapshot value = cache;
        if (value != null && modified == cacheModified && now - cacheTime < CACHE_TTL_MILLIS) {
            return value.copy();
        }
        synchronized (LOCK) {
            value = cache;
            if (value != null && modified == cacheModified
                    && System.currentTimeMillis() - cacheTime < CACHE_TTL_MILLIS) {
                return value.copy();
            }
            Snapshot loaded = load(file);
            cache = loaded;
            cacheModified = file.exists() ? file.lastModified() : -1;
            cacheTime = System.currentTimeMillis();
            return loaded.copy();
        }
    }

    /** 绕过缓存读取磁盘最新内容，用于写盘前合并其它进程的改动 */
    public static Snapshot readFresh() {
        synchronized (LOCK) {
            File file = getFile();
            Snapshot loaded = load(file);
            cache = loaded;
            cacheModified = file.exists() ? file.lastModified() : -1;
            cacheTime = System.currentTimeMillis();
            return loaded.copy();
        }
    }

    public static boolean isAccountSwitchEnabled() {
        try {
            return read().isAccountSwitchEnabled();
        } catch (Throwable t) {
            return false;
        }
    }

    public static int getAccountSwitchIntervalMinutes() {
        try {
            return read().getAccountSwitchIntervalMinutes();
        } catch (Throwable t) {
            return 0;
        }
    }

    public static int getAccountSwitchRoundIntervalMinutes() {
        try {
            return read().getAccountSwitchRoundIntervalMinutes();
        } catch (Throwable t) {
            return 0;
        }
    }

    public static List<Account> getAccountSwitchAccounts() {
        try {
            return read().getAccountSwitchAccounts();
        } catch (Throwable t) {
            return new ArrayList<>();
        }
    }

    public static String getAccountSwitchFinalAccountUid() {
        try {
            return read().getAccountSwitchFinalAccountUid();
        } catch (Throwable t) {
            return "";
        }
    }

    public static List<String> getAccountSwitchScheduleTimes() {
        try {
            return read().getAccountSwitchScheduleTimes();
        } catch (Throwable t) {
            return new ArrayList<>();
        }
    }

    public static synchronized boolean setAccountSwitchEnabled(boolean enabled) {
        return mutateAndWrite(Field.ENABLED, s -> s.setAccountSwitchEnabled(enabled));
    }

    public static synchronized boolean setAccountSwitchIntervalMinutes(int minutes) {
        int clamped = clampMinutes(minutes);
        return mutateAndWrite(Field.INTERVAL, s -> s.setAccountSwitchIntervalMinutes(clamped));
    }

    public static synchronized boolean setAccountSwitchRoundIntervalMinutes(int minutes) {
        int clamped = clampMinutes(minutes);
        return mutateAndWrite(Field.ROUND_INTERVAL, s -> s.setAccountSwitchRoundIntervalMinutes(clamped));
    }

    /** 按传入顺序整体替换账号列表（界面上下移动后保存） */
    public static synchronized boolean setAccountSwitchAccounts(List<Account> accounts) {
        List<Account> normalized = normalize(accounts);
        return mutateAndWrite(Field.ACCOUNTS, s -> s.setAccountSwitchAccounts(normalized));
    }

    public static synchronized boolean setAccountSwitchFinalAccountUid(String uid) {
        return mutateAndWrite(Field.FINAL_ACCOUNT, s -> s.setAccountSwitchFinalAccountUid(uid));
    }

    public static synchronized boolean setAccountSwitchScheduleTimes(List<String> times) {
        List<String> normalized = normalizeScheduleTimes(times);
        return mutateAndWrite(Field.SCHEDULE_TIMES, s -> s.setAccountSwitchScheduleTimes(normalized));
    }

    /**
     * 改一个字段并落盘。
     * <p>写盘前**重新读一次磁盘**，只把本次真正改动的那个字段套到最新磁盘内容上：
     * 模块 App 进程与支付宝注入进程共用这一个文件，两边的 {@code synchronized}
     * 互不相干。原先是「readFresh → 改字段 → write」三步，
     * 另一个进程若在这两步之间改了同一文件，本次写回就会把它的改动覆盖掉
     * （表现为用户在设置里改的「切号等待间隔」被注入进程悄悄还原成默认值）。
     * 真正要彻底消除丢更新需要文件锁，但两个进程都要改，代价大，这里先合并到最新磁盘状态。
     *
     * @param field   本次改动的字段
     * @param mutator 在快照上施加改动
     */
    private static boolean mutateAndWrite(Field field, java.util.function.Consumer<Snapshot> mutator) {
        Snapshot snapshot = readFresh();
        mutator.accept(snapshot);
        // 再读一次磁盘拿到别的进程写入的内容，只把本次改动的字段覆盖上去
        Snapshot latest = load(getFile());
        field.applyTo(latest, snapshot);
        return write(latest);
    }

    /** 可写字段：用于跨进程写盘时只合并本次真正改动的那一项 */
    private enum Field {
        ENABLED {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchEnabled(source.isAccountSwitchEnabled());
            }
        },
        INTERVAL {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchIntervalMinutes(source.getAccountSwitchIntervalMinutes());
            }
        },
        ROUND_INTERVAL {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchRoundIntervalMinutes(source.getAccountSwitchRoundIntervalMinutes());
            }
        },
        ACCOUNTS {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchAccounts(source.getAccountSwitchAccounts());
            }
        },
        FINAL_ACCOUNT {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchFinalAccountUid(source.getAccountSwitchFinalAccountUid());
            }
        },
        SCHEDULE_TIMES {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAccountSwitchScheduleTimes(source.getAccountSwitchScheduleTimes());
            }
        },
        AUTO_SLIDE_CAPTCHA {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setAutoSlideCaptchaEnabled(source.isAutoSlideCaptchaEnabled());
            }
        },
        UNLOCK_ACCOUNT_LIMIT {
            @Override
            void applyTo(Snapshot target, Snapshot source) {
                target.setUnlockAccountLimitEnabled(source.isUnlockAccountLimitEnabled());
            }
        };

        abstract void applyTo(Snapshot target, Snapshot source);
    }

    /** 是否自动处理滑动/拼图验证（默认开启） */
    public static boolean isAutoSlideCaptchaEnabled() {
        try {
            return read().isAutoSlideCaptchaEnabled();
        } catch (Throwable t) {
            return true;
        }
    }

    /** 是否解除登录账号数量限制（默认开启） */
    public static boolean isUnlockAccountLimitEnabled() {
        try {
            return read().isUnlockAccountLimitEnabled();
        } catch (Throwable t) {
            return true;
        }
    }

    /**
     * 是否需要保护本地账号登录记录。
     * 解除账号数量限制或自动切号任一开启时都必须保护，
     * 否则账号超过 5 个会被账号管理页清理，导致切号列表失效。
     */
    public static boolean isAccountListProtected() {
        return isUnlockAccountLimitEnabled() || isAccountSwitchEnabled();
    }

    public static synchronized boolean setAutoSlideCaptchaEnabled(boolean enabled) {
        return mutateAndWrite(Field.AUTO_SLIDE_CAPTCHA, s -> s.setAutoSlideCaptchaEnabled(enabled));
    }

    public static synchronized boolean setUnlockAccountLimitEnabled(boolean enabled) {
        return mutateAndWrite(Field.UNLOCK_ACCOUNT_LIMIT, s -> s.setUnlockAccountLimitEnabled(enabled));
    }

    /**
     * 模块启动时把支付宝已登录账号合并进配置：已有账号保持原位置，新账号追加到末尾。
     */
    public static synchronized boolean mergeHostAccounts(List<Account> hostAccounts) {
        if (hostAccounts == null || hostAccounts.isEmpty()) {
            return false;
        }
        Snapshot snapshot = readFresh();
        LinkedHashMap<String, Account> merged = new LinkedHashMap<>();
        for (Account account : snapshot.getAccountSwitchAccounts()) {
            if (account != null && isValidUid(account.getUid())) {
                merged.put(account.getUid(), account);
            }
        }
        boolean changed = false;
        for (Account host : hostAccounts) {
            if (host == null || !isValidUid(host.getUid())) {
                continue;
            }
            Account existing = merged.get(host.getUid());
            if (existing == null) {
                merged.put(host.getUid(), new Account(host.getUid(), host.getLoginId(), host.getName()));
                changed = true;
                continue;
            }
            if (!StringUtil.isEmpty(host.getLoginId())
                    && !host.getLoginId().equals(existing.getLoginId())) {
                existing.setLoginId(host.getLoginId());
                changed = true;
            }
            if (!StringUtil.isEmpty(host.getName()) && !host.getName().equals(existing.getName())) {
                existing.setName(host.getName());
                changed = true;
            }
        }
        if (!changed) {
            return true;
        }
        /*
         * 合并结果必须写回快照再落盘。
         * <p>早先只把 merged 放进局部变量、却没写回 snapshot，导致新登录的账号从未真正写进
         * config_common.json —— 界面上「基础设置」里的账号列表始终只有旧的那几个。
         */
        snapshot.setAccountSwitchAccounts(new ArrayList<>(merged.values()));
        /*
         * 写盘前再读一次磁盘，只把账号列表这一项套到最新磁盘内容上。
         * 本方法只负责账号列表，却会把整份配置写回；
         * 不做这一步的话，用户刚在设置里改的「切号等待间隔」等会被这里的旧快照覆盖掉
         * —— 表现为设置改了不生效。
         */
        Snapshot latest = load(getFile());
        Field.ACCOUNTS.applyTo(latest, snapshot);
        return write(latest);
    }

    private static Snapshot load(File file) {
        Snapshot snapshot = new Snapshot();
        if (file == null || !file.exists() || !file.isFile()) {
            // 文件不存在：账号列表由调度器在后台线程补全，避免在主线程反射调用支付宝服务导致宿主卡死
            return snapshot;
        }
        String text = FileUtil.readFromFile(file);
        if (StringUtil.isEmpty(text)) {
            return snapshot;
        }
        try {
            JSONObject json = new JSONObject(text);
            snapshot.setAccountSwitchEnabled(json.optBoolean("accountSwitchEnabled", false));
            snapshot.setAccountSwitchIntervalMinutes(
                    clampMinutes(json.optInt("accountSwitchIntervalMinutes", 0)));
            snapshot.setAccountSwitchRoundIntervalMinutes(
                    clampMinutes(json.optInt("accountSwitchRoundIntervalMinutes", 0)));
            snapshot.setAccountSwitchFinalAccountUid(
                    json.optString("accountSwitchFinalAccountUid", ""));
            List<String> scheduleTimes = new ArrayList<>();
            JSONArray scheduleArray = json.optJSONArray("accountSwitchScheduleTimes");
            if (scheduleArray != null) {
                for (int i = 0; i < scheduleArray.length(); i++) {
                    Object item = scheduleArray.opt(i);
                    if (item != null) {
                        scheduleTimes.add(String.valueOf(item));
                    }
                }
            }
            snapshot.setAccountSwitchScheduleTimes(scheduleTimes);
            snapshot.setAutoSlideCaptchaEnabled(json.optBoolean("autoSlideCaptchaEnabled", true));
            snapshot.setUnlockAccountLimitEnabled(json.optBoolean("unlockAccountLimitEnabled", true));
            JSONArray array = json.optJSONArray("accountSwitchAccounts");
            List<Account> accounts = new ArrayList<>();
            if (array != null) {
                for (int i = 0; i < array.length(); i++) {
                    Object item = array.opt(i);
                    if (item instanceof JSONObject) {
                        JSONObject object = (JSONObject) item;
                        accounts.add(new Account(
                                object.optString("uid", null),
                                object.optString("loginId", null),
                                object.optString("name", null)));
                    } else if (item != null) {
                        accounts.add(new Account(String.valueOf(item), null, null));
                    }
                }
            }
            snapshot.setAccountSwitchAccounts(normalize(accounts));
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#解析 config_common.json 失败", t);
            /*
             * 解析失败时**保留上一次读到的有效配置**，不要回落默认值：
             * 回落默认值会让「自动切号开关」变成关、「切号等待间隔」变成 0，
             * 而紧接着的任何一次 write 都会把这份默认值写回磁盘，
             * 用户的切号配置就被静默清空了。
             */
            Snapshot previous = cache;
            if (previous != null) {
                return previous.copy();
            }
            snapshot.setAccountSwitchAccounts(new ArrayList<>());
        }
        return snapshot;
    }

    private static boolean write(Snapshot snapshot) {
        try {
            JSONObject json = new JSONObject();
            json.put("accountSwitchEnabled", snapshot.isAccountSwitchEnabled());
            json.put("accountSwitchIntervalMinutes",
                    clampMinutes(snapshot.getAccountSwitchIntervalMinutes()));
            json.put("accountSwitchRoundIntervalMinutes",
                    clampMinutes(snapshot.getAccountSwitchRoundIntervalMinutes()));
            json.put("accountSwitchFinalAccountUid",
                    snapshot.getAccountSwitchFinalAccountUid());
            JSONArray scheduleArray = new JSONArray();
            for (String time : snapshot.getAccountSwitchScheduleTimes()) {
                scheduleArray.put(time);
            }
            json.put("accountSwitchScheduleTimes", scheduleArray);
            json.put("autoSlideCaptchaEnabled", snapshot.isAutoSlideCaptchaEnabled());
            json.put("unlockAccountLimitEnabled", snapshot.isUnlockAccountLimitEnabled());
            JSONArray array = new JSONArray();
            for (Account account : snapshot.getAccountSwitchAccounts()) {
                JSONObject object = new JSONObject();
                object.put("uid", account.getUid());
                if (!StringUtil.isEmpty(account.getLoginId())) {
                    object.put("loginId", account.getLoginId());
                }
                if (!StringUtil.isEmpty(account.getName())) {
                    object.put("name", account.getName());
                }
                array.put(object);
            }
            json.put("accountSwitchAccounts", array);
            File file = getFile();
            boolean success = FileUtil.write2File(json.toString(2), file);
            if (success) {
                cache = snapshot.copy();
                cacheModified = file.exists() ? file.lastModified() : -1;
                cacheTime = System.currentTimeMillis();
            }
            return success;
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#保存 config_common.json 失败", t);
            return false;
        }
    }

    /** 过滤非法时间点并去重，保留用户输入顺序。支持 2/4/6 位：HH、HHmm、HHmmss */
    private static List<String> normalizeScheduleTimes(List<String> source) {
        List<String> result = new ArrayList<>();
        if (source == null) {
            return result;
        }
        for (String item : source) {
            if (item == null) {
                continue;
            }
            String time = item.trim();
            if (!isValidTimeStr(time) || result.contains(time)) {
                continue;
            }
            result.add(time);
        }
        return result;
    }

    private static boolean isValidTimeStr(String time) {
        int length = time.length();
        if (length != 2 && length != 4 && length != 6) {
            return false;
        }
        for (int i = 0; i < length; i++) {
            if (!Character.isDigit(time.charAt(i))) {
                return false;
            }
        }
        try {
            int hour = Integer.parseInt(time.substring(0, 2));
            int minute = length >= 4 ? Integer.parseInt(time.substring(2, 4)) : 0;
            int second = length == 6 ? Integer.parseInt(time.substring(4, 6)) : 0;
            return hour >= 0 && hour <= 23
                    && minute >= 0 && minute <= 59
                    && second >= 0 && second <= 59;
        } catch (Exception e) {
            return false;
        }
    }

    private static List<Account> normalize(List<Account> source) {
        LinkedHashMap<String, Account> unique = new LinkedHashMap<>();
        if (source != null) {
            for (Account account : source) {
                if (account == null) {
                    continue;
                }
                String uid = account.getUid() == null ? null : account.getUid().trim();
                if (!isValidUid(uid) || unique.containsKey(uid)) {
                    continue;
                }
                String loginId = StringUtil.isEmpty(account.getLoginId())
                        ? null : account.getLoginId().trim();
                String name = StringUtil.isEmpty(account.getName())
                        ? null : account.getName().trim();
                unique.put(uid, new Account(uid, loginId, name));
            }
        }
        return new ArrayList<>(unique.values());
    }

    private static boolean isValidUid(String uid) {
        if (StringUtil.isEmpty(uid) || uid.length() > 128) {
            return false;
        }
        for (int i = 0; i < uid.length(); i++) {
            char c = uid.charAt(i);
            boolean valid = (c >= '0' && c <= '9')
                    || (c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || c == '_' || c == '@' || c == '.' || c == '+' || c == ':' || c == '-';
            if (!valid) {
                return false;
            }
        }
        return true;
    }

    private static int clampMinutes(int minutes) {
        if (minutes < 0) {
            return 0;
        }
        return Math.min(minutes, MAX_INTERVAL_MINUTES);
    }

    /** 一个支付宝登录账号：uid 为账号唯一标识，loginId 为登录名（界面显示用） */
    public static final class Account {

        private String uid;
        private String loginId;
        private String name;

        public Account() {
        }

        public Account(String uid, String loginId, String name) {
            this.uid = uid;
            this.loginId = loginId;
            this.name = name;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getLoginId() {
            return loginId;
        }

        public void setLoginId(String loginId) {
            this.loginId = loginId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        /** 界面统一显示登录名，其次显示昵称，最后退回 uid */
        public String getDisplayName() {
            if (!StringUtil.isEmpty(loginId)) {
                return loginId;
            }
            if (!StringUtil.isEmpty(name)) {
                return name;
            }
            return StringUtil.isEmpty(uid) ? "未知" : uid;
        }

        public Account copy() {
            return new Account(uid, loginId, name);
        }
    }

    /** 一份配置快照：读取时返回副本，避免调用方就地改到缓存 */
    public static final class Snapshot {

        private boolean accountSwitchEnabled;
        private int accountSwitchIntervalMinutes;
        private int accountSwitchRoundIntervalMinutes;
        private String accountSwitchFinalAccountUid = "";
        private List<String> accountSwitchScheduleTimes = new ArrayList<>();
        private List<Account> accountSwitchAccounts = new ArrayList<>();
        /** 自动处理滑动/拼图验证，默认开启 */
        private boolean autoSlideCaptchaEnabled = true;
        /** 解除登录账号数量限制，默认开启 */
        private boolean unlockAccountLimitEnabled = true;

        public boolean isAutoSlideCaptchaEnabled() {
            return autoSlideCaptchaEnabled;
        }

        public void setAutoSlideCaptchaEnabled(boolean autoSlideCaptchaEnabled) {
            this.autoSlideCaptchaEnabled = autoSlideCaptchaEnabled;
        }

        public boolean isUnlockAccountLimitEnabled() {
            return unlockAccountLimitEnabled;
        }

        public void setUnlockAccountLimitEnabled(boolean unlockAccountLimitEnabled) {
            this.unlockAccountLimitEnabled = unlockAccountLimitEnabled;
        }

        public boolean isAccountSwitchEnabled() {
            return accountSwitchEnabled;
        }

        public void setAccountSwitchEnabled(boolean accountSwitchEnabled) {
            this.accountSwitchEnabled = accountSwitchEnabled;
        }

        public int getAccountSwitchIntervalMinutes() {
            return accountSwitchIntervalMinutes;
        }

        public void setAccountSwitchIntervalMinutes(int accountSwitchIntervalMinutes) {
            this.accountSwitchIntervalMinutes = clampMinutes(accountSwitchIntervalMinutes);
        }

        public int getAccountSwitchRoundIntervalMinutes() {
            return accountSwitchRoundIntervalMinutes;
        }

        public void setAccountSwitchRoundIntervalMinutes(int accountSwitchRoundIntervalMinutes) {
            this.accountSwitchRoundIntervalMinutes = clampMinutes(accountSwitchRoundIntervalMinutes);
        }

        public List<Account> getAccountSwitchAccounts() {
            return accountSwitchAccounts;
        }

        public void setAccountSwitchAccounts(List<Account> accountSwitchAccounts) {
            this.accountSwitchAccounts =
                    accountSwitchAccounts == null ? new ArrayList<>() : accountSwitchAccounts;
        }

        public String getAccountSwitchFinalAccountUid() {
            return accountSwitchFinalAccountUid;
        }

        public void setAccountSwitchFinalAccountUid(String accountSwitchFinalAccountUid) {
            this.accountSwitchFinalAccountUid =
                    accountSwitchFinalAccountUid == null ? "" : accountSwitchFinalAccountUid;
        }

        public List<String> getAccountSwitchScheduleTimes() {
            return accountSwitchScheduleTimes;
        }

        public void setAccountSwitchScheduleTimes(List<String> accountSwitchScheduleTimes) {
            this.accountSwitchScheduleTimes = accountSwitchScheduleTimes == null
                    ? new ArrayList<>() : accountSwitchScheduleTimes;
        }

        public Snapshot copy() {
            Snapshot copy = new Snapshot();
            copy.accountSwitchEnabled = accountSwitchEnabled;
            copy.accountSwitchIntervalMinutes = accountSwitchIntervalMinutes;
            copy.accountSwitchRoundIntervalMinutes = accountSwitchRoundIntervalMinutes;
            copy.accountSwitchFinalAccountUid = accountSwitchFinalAccountUid;
            copy.accountSwitchScheduleTimes = new ArrayList<>(accountSwitchScheduleTimes);
            List<Account> list = new ArrayList<>();
            for (Account account : accountSwitchAccounts) {
                if (account != null) {
                    list.add(account.copy());
                }
            }
            copy.accountSwitchAccounts = list;
            copy.autoSlideCaptchaEnabled = autoSlideCaptchaEnabled;
            copy.unlockAccountLimitEnabled = unlockAccountLimitEnabled;
            return copy;
        }
    }
}
