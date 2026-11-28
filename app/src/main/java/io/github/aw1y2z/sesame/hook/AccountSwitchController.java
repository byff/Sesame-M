package io.github.aw1y2z.sesame.hook;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.aw1y2z.sesame.data.AccountSwitchConfig;
import io.github.aw1y2z.sesame.data.task.ModelTask;
import io.github.aw1y2z.sesame.model.normal.base.BaseModel;
import io.github.aw1y2z.sesame.util.Log;
import io.github.aw1y2z.sesame.util.StringUtil;
import io.github.aw1y2z.sesame.util.TimeUtil;

/**
 * 自动切换登录账号调度器。
 *
 * 触发条件：开关开启 + 当前账号任务全部结束（不含未来到点的定时子任务）
 *          + 当前账号运行时长达到「切号等待间隔」。
 * 轮次：一轮 = 所有可用账号各访问一次，收尾在常态保持账号上；走完后等轮次间隔再开始下一轮。
 * 切号顺序：按 config_common.json 中的账号列表顺序循环；
 *          列表走完（回到开头）时，如果配置了「常态保持账号」，先切到该账号执行一次，作为一轮结束；
 *          轮次等待从切入常态保持账号时开始计时，期间该账号继续按自己的执行间隔跑任务但不切号，
 *          等待时间到且本轮任务跑完后切到下一轮账号；重启后首次运行时视为等待时间已过，跑完直接切；
 *          未配置常态保持账号时，回到列表第一个账号等待轮次间隔。
 * 定时切号：config_common.json 中配置的「切换账号定时执行」到点后，立即终止当前账号任务，
 *          固定等待 20 秒后切换到常态保持账号并执行一轮（设置方式与定时执行相同：HH/HHmm/HHmmss，逗号分隔）。
 */
public final class AccountSwitchController {

    public interface Host {
        String currentUid();

        boolean isReadyForSwitch();

        /** 初始化指定账号；startMain=false 时只加载配置，不启动任务 */
        boolean initialize(String uid, boolean startMain);

        void pause();

        void resume();
    }

    private static final String TAG = "AccountSwitch";

    /** 任务刚结束时留一点缓冲，避免任务链切换间隙误判为全部完成 */
    private static final long IDLE_GRACE_MILLIS = 5_000L;
    private static final long SWITCH_POLL_INTERVAL_MILLIS = 500L;
    private static final int SWITCH_POLL_COUNT = 40;
    private static final long FAILED_RETRY_MILLIS = 60_000L;
    /** 目标账号需要重新登录时，重试没有意义，拉长到 10 分钟再试一次 */
    private static final long MANUAL_LOGIN_RETRY_MILLIS = 10 * 60_000L;
    /** 轮次等待期间输出等待状态的间隔 */
    private static final long ROUND_WAIT_LOG_INTERVAL_MILLIS = 30 * 60_000L;
    /** 账号列表不足时补充读取支付宝账号的最小间隔 */
    private static final long ACCOUNT_SYNC_INTERVAL_MILLIS = 30_000L;
    /** 账号列表够用时的定期复核间隔：新登录的账号会自动追加到列表末尾 */
    private static final long ACCOUNT_RESYNC_INTERVAL_MILLIS = 30 * 60_000L;
    /** 启动后延迟读取支付宝账号，等宿主框架就绪，避免和宿主启动流程抢服务 */
    private static final long STARTUP_SYNC_DELAY_MILLIS = 10_000L;
    /** 进程启动后的冷却时间，期间不切号，避免干扰支付宝自身的启动/登录流程 */
    private static final long STARTUP_SETTLE_MILLIS = 60_000L;
    /**
     * 两次切号之间的最小间隔：防止「一轮跑完就切」切得过快。
     * <p>切号会重载账号配置、重新登录支付宝并再跑一轮，
     * 间隔太短会让支付宝反复切换登录态，容易触发风控；
     * 同时也保证新旧账号的登录态有足够时间稳定下来。
     */
    private static final long MIN_SWITCH_GAP_MILLIS = 3 * 60_000L;
    /**
     * 切号前等待旧账号任务真正退出的超时。
     * <p>不等待就会让旧账号的收尾动作与新账号的任务重叠（两个账号同时处理同一个任务）。
     */
    private static final long WAIT_TASK_STOPPED_MILLIS = 30_000L;
    /** 定时切号：终止当前任务后到切换之间的固定间隔（20 秒） */
    private static final long SCHEDULE_SWITCH_DELAY_MILLIS = 20_000L;
    /** 定时切号失败后的最大重试次数（每次间隔 FAILED_RETRY_MILLIS） */
    private static final int SCHEDULED_MAX_RETRY = 3;

    private static final Object LOCK = new Object();

    private static volatile Host host;
    private static volatile boolean paused;
    private static volatile boolean switching;
    private static volatile boolean awaitingResume;
    private static volatile long resumeAt;
    /** 轮次等待结束后要切入的账号；为空表示等待结束直接恢复当前账号 */
    private static volatile String pendingSwitchUid;
    private static volatile long runStartTime;
    private static volatile long tasksStartedAt;
    private static volatile long idleSince;
    private static volatile long nextAttemptAt;
    /**
     * 当前驻留账号的 uid：用于判断「是不是换账号了」。
     * <p>只有换账号才重置 {@link #residentSince}，重载配置不算换号。
     */
    private static volatile String residentUid;
    /**
     * 切入当前账号的时刻：切号判定「这个账号已经待了多久」的基准。
     * <p>与 {@link #runStartTime} 的区别：runStartTime 是「本轮任务开始运行」，
     * 每次重载/恢复都会刷新；residentSince 是「切入这个账号」，只有换账号才刷新。
     */
    private static volatile long residentSince;
    /** 本账号是否已经输出过「等待切号间隔」的提示，避免重复刷日志 */
    private static volatile boolean intervalWaitLogged;
    /** 上一次输出轮次等待状态的时间 */
    private static volatile long roundWaitLastLogAt;
    private static volatile long lastAccountSync;
    private static volatile long lastSwitchEndAt;
    /**
     * 本轮已访问过的账号 uid 集合。
     * <p>一轮 = 把所有可用账号各访问一次。用集合记录进度，
     * 比「锚点 + 下标推算」稳得多：启动时停在哪个账号、
     * 中途账号登录记录失效、列表顺序调整，都不会把访问进度算错。
     * <p>切走账号 X 说明 X 这一轮的任务已跑完，X 计入本集合；
     * 目标是常态保持账号、且本集合已覆盖所有可用账号时，这一轮才算走完。
     */
    private static final Set<String> roundVisited = new HashSet<>();
    /**
     * 本轮所有账号都跑完的时刻：轮次间隔从这里起算。
     * <p>0 表示本轮还没走完。轮次间隔的语义是「所有账号轮完一圈后等多久」，
     * 计时起点必须是圈跑完的那一刻，不能从切入第一个账号时算
     * （那会把各账号自己跑任务的时间白白吃掉）。
     */
    private static volatile long roundFinishedAt;
    /** 定时切号已触发并终止任务，到该时刻后切换到保持账号；0 表示没有待执行的定时切号 */
    private static volatile long scheduledSwitchAt;
    /** 上一次检查定时切号时间点的时刻，用于判断时间点是否落在两次检查之间 */
    private static volatile long lastScheduleCheckAt;
    /** 定时切号失败后的重试计数，成功后清零 */
    private static volatile int scheduledRetryCount;
    /** 已触发过的定时切号时间点（日期+时间），避免同一个时间点重复触发 */
    private static final Set<String> firedSchedulePoints = new HashSet<>();
    private static volatile long scheduleDayStart;

    private static volatile long processStartAt;

    private static final AtomicBoolean syncRequested = new AtomicBoolean();

    private static ScheduledExecutorService scheduler;
    private static ExecutorService switchExecutor;
    private static ExecutorService syncExecutor;

    private AccountSwitchController() {
    }

    public static void configure(Host newHost) {
        synchronized (LOCK) {
            host = newHost;
            if (processStartAt <= 0) {
                processStartAt = System.currentTimeMillis();
            }
            if (scheduler == null) {
                scheduler = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
                scheduler.scheduleWithFixedDelay(AccountSwitchController::tick,
                        0, 1, TimeUnit.SECONDS);
            }
            if (switchExecutor == null) {
                switchExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
            }
            if (syncExecutor == null) {
                syncExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory());
            }
        }
    }

    /** 主任务被暂停（切号中/轮次等待中）时，ApplicationHook 不应执行新一轮任务 */
    public static boolean isPaused() {
        return paused;
    }

    /**
     * 切号是否正在执行。
     * <p>比 {@link #isPaused()} 更精确：paused 在「切号中」和「轮次等待中」都为真，
     * 而只有切号真正在跑时，外部的重载请求才需要被拦下。
     */
    public static boolean isSwitching() {
        return switching;
    }

    /**
     * 是否处于「轮次等待」中：一轮已走完并切回了常态保持账号，
     * 正等着「切号轮次间隔」再开始下一轮。
     * <p>等待期间常态保持账号**不执行任务** —— 切回来就再跑一轮不是期望的行为。
     */
    public static boolean isRoundWaiting() {
        return awaitingResume;
    }

    /**
     * 账号开始运行（没有换账号）：刷新运行起点即可。
     * <p>驻留计时与轮次等待状态**不能**在这里重置：
     * 重载配置（{@code runInit}）也会走到这里，那不是换账号。
     * 原实现在这里清空了 awaitingResume / pendingSwitchUid，
     * 于是「保存配置」恰好落在轮次等待中时，等待被静默取消，常态保持账号再也不会切走。
     */
    public static void onRunResumed(String uid) {
        long now = System.currentTimeMillis();
        runStartTime = now;
        tasksStartedAt = 0;
        idleSince = 0;
        intervalWaitLogged = false;
        if (!StringUtil.isEmpty(uid) && !uid.equals(residentUid)) {
            // 以支付宝实际登录账号为准复核一次：切号过程中传进来的 uid 可能还是旧账号，
            // 那时若直接重置驻留计时，会把「刚切入新账号」的计时清零，切号间隔永远不满。
            String actual = AlipayAccountSwitchBridge.currentUid();
            if (!StringUtil.isEmpty(actual) && !actual.equals(residentUid)) {
                residentUid = actual;
                residentSince = now;
                Log.record("自动切号🔄️ 检测到登录账号变化为 " + label(actual) + "，重新开始计时");
            }
        }
        Log.record("自动切号🔄️ 账号开始运行: " + label(uid));
    }

    /**
     * 切号完成：登录账号已经换成 {@code uid}。
     * <p>驻留计时从这里重新开始 —— 「切号等待间隔」的语义就是「每个账号至少待多久」，
     * 必须从切入该账号的时刻算起，而不是从任务跑完那刻算。
     */
    public static void onAccountSwitched(String uid) {
        long now = System.currentTimeMillis();
        residentUid = uid;
        residentSince = now;
        runStartTime = now;
        tasksStartedAt = 0;
        idleSince = 0;
        nextAttemptAt = 0;
        awaitingResume = false;
        resumeAt = 0;
        pendingSwitchUid = null;
        intervalWaitLogged = false;
        roundWaitLastLogAt = 0;
        Log.record("自动切号🔄️ 账号开始运行: " + label(uid));
    }

    /** 一轮任务已下发，允许开始判断任务是否全部结束 */
    public static void onTasksStarted() {
        tasksStartedAt = System.currentTimeMillis();
        idleSince = 0;
    }

    /**
     * 请求在后台线程同步支付宝登录账号。
     * 反射调用支付宝服务比较重，禁止在宿主主线程直接执行（会造成支付宝卡死）。
     */
    public static void requestAccountSync() {
        ExecutorService executor = syncExecutor;
        if (executor == null) {
            return;
        }
        if (!syncRequested.compareAndSet(false, true)) {
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    Thread.sleep(STARTUP_SYNC_DELAY_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } finally {
                    syncRequested.set(false);
                }
                syncAccountsFromHost();
            });
        } catch (Throwable t) {
            syncRequested.set(false);
            Log.printStackTrace(TAG + "#提交账号同步失败", t);
        }
    }

    /**
     * 立即同步支付宝登录账号（界面点「刷新」时用）。
     * <p>与 {@link #requestAccountSync()} 的区别是不等启动延迟：用户刚在支付宝里登录了新账号，
     * 希望马上在列表里看到。
     */
    public static void requestAccountSyncNow() {
        ExecutorService executor = syncExecutor;
        if (executor == null) {
            // 调度器还没起来（宿主刚启动）：退化为直接跑一次，仍在后台线程
            syncAccountsFromHost();
            return;
        }
        try {
            executor.execute(AccountSwitchController::syncAccountsFromHost);
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#提交账号同步失败", t);
        }
    }

    /** 模块启动时把支付宝登录账号合并到公共配置，已有顺序不变，新账号追加 */
    public static void syncAccountsFromHost() {
        lastAccountSync = System.currentTimeMillis();
        try {
            List<AccountSwitchConfig.Account> accounts =
                    AlipayAccountSwitchBridge.fetchAccountsForConfig();
            if (accounts.isEmpty()) {
                Log.record("自动切号🔄️ 未读取到支付宝登录账号");
                return;
            }
            AccountSwitchConfig.mergeHostAccounts(accounts);
            Log.record("自动切号🔄️ 账号列表已同步，共 "
                    + AccountSwitchConfig.getAccountSwitchAccounts().size() + " 个账号");
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#同步登录账号失败", t);
        }
    }

    private static void tick() {
        try {
            Host currentHost = host;
            if (currentHost == null) {
                return;
            }
            AccountSwitchConfig.Snapshot config = AccountSwitchConfig.read();
            if (!config.isAccountSwitchEnabled()) {
                scheduledSwitchAt = 0;
                // 开关关闭期间不参与时间点判定，重新开启后也不补触发关闭期间错过的时间点
                lastScheduleCheckAt = 0;
                if (paused || awaitingResume) {
                    paused = false;
                    awaitingResume = false;
                    pendingSwitchUid = null;
                    roundWaitLastLogAt = 0;
                    // 轮次状态一并复位：重新开启时从当前账号重新起算一轮
                    roundVisited.clear();
                    roundFinishedAt = 0L;
                    currentHost.resume();
                }
                return;
            }
            long now = System.currentTimeMillis();
            // 定时切号优先：不管当前处于什么状态，到点立即终止任务，20 秒后切到保持账号
            if (checkScheduleTrigger(config, now)) {
                beginScheduledSwitch(currentHost);
                return;
            }
            if (switching) {
                return;
            }
            // 定时切号：任务已终止，固定等待 20 秒后切到保持账号
            if (scheduledSwitchAt > 0) {
                if (now < scheduledSwitchAt) {
                    return;
                }
                scheduledSwitchAt = 0;
                paused = true;
                switching = true;
                try {
                    switchExecutor.execute(AccountSwitchController::switchToHoldAndRun);
                } catch (Throwable t) {
                    Log.printStackTrace(TAG + "#定时切号提交失败", t);
                    switching = false;
                    paused = false;
                }
                return;
            }
            if (awaitingResume) {
                if (now >= resumeAt) {
                    String pendingUid = pendingSwitchUid;
                    if (!ModelTask.isAllTaskFinished()) {
                        /*
                         * 保持账号还在按自己的执行间隔跑任务，等这一轮跑完再切号，避免中途打断。
                         * <p>注意不要把 resumeAt 往后推：resumeAt 是「等待结束的时刻」，
                         * 推它就等于把轮次间隔无限延长，等待永远等不到头。
                         * 这里只挪动「下次检查的时刻」，用单独变量记。
                         */
                        return;
                    }
                    pendingSwitchUid = null;
                    awaitingResume = false;
                    roundWaitLastLogAt = 0;
                    if (!StringUtil.isEmpty(pendingUid) && !pendingUid.equals(currentHost.currentUid())) {
                        /*
                         * 锚点不在这里改：切换可能失败（账号无登录记录/被支付宝拒绝），
                         * 提前改了会让锚点指向一个没切过去的账号，
                         * 下一轮就会被误判成「已走完」，那个账号整轮被跳过。
                         * 由 resumeSwitch 在切换成功后统一设。
                         */
                        roundFinishedAt = 0L;
                        // 轮次等待结束：从保持账号切入下一轮账号并立即开始执行。
                        // 先暂停再提交切号，避免提交到真正执行之间又拉起新一轮任务
                        final String targetUid = pendingUid;
                        try {
                            paused = true;
                            switching = true;
                            switchExecutor.execute(() -> resumeSwitch(targetUid));
                            return;
                        } catch (Throwable t) {
                            Log.printStackTrace(TAG + "#轮次切号提交失败", t);
                            switching = false;
                            paused = false;
                        }
                    }
                    paused = false;
                    onRunResumed(currentHost.currentUid());
                    currentHost.resume();
                    Log.record("自动切号🔄️ 轮次等待结束，开始运行");
                } else if (now - roundWaitLastLogAt >= ROUND_WAIT_LOG_INTERVAL_MILLIS) {
                    roundWaitLastLogAt = now;
                    long remainMinutes = (resumeAt - now + 59_999L) / 60_000L;
                    Log.record("自动切号🔄️ 轮次等待中，当前保持账号: "
                            + label(currentHost.currentUid())
                            + "，预计还需 " + remainMinutes + " 分钟");
                }
                return;
            }
            // 账号列表不足（例如先在设置界面建了配置文件）时定期从支付宝补全，新登录的账号会追加到末尾
            if (config.getAccountSwitchAccounts().size() < 2) {
                if (processStartAt > 0 && now - processStartAt < STARTUP_SYNC_DELAY_MILLIS) {
                    return;
                }
                if (now - lastAccountSync >= ACCOUNT_SYNC_INTERVAL_MILLIS) {
                    syncAccountsFromHost();
                }
                return;
            }
            // 列表够用时也定期复核：新登录的账号会自动追加到末尾（顺序不变）
            if (now - lastAccountSync >= ACCOUNT_RESYNC_INTERVAL_MILLIS) {
                syncAccountsFromHost();
            }
            if (!currentHost.isReadyForSwitch()) {
                idleSince = 0;
                return;
            }
            // 进程刚启动时先让支付宝自己的启动/登录流程跑完
            if (processStartAt > 0 && now - processStartAt < STARTUP_SETTLE_MILLIS) {
                return;
            }
            if (runStartTime <= 0) {
                runStartTime = now;
                return;
            }
            if (tasksStartedAt <= 0) {
                return;
            }
            if (!ModelTask.isAllTaskFinished()) {
                idleSince = 0;
                intervalWaitLogged = false;
                return;
            }
            if (idleSince <= 0) {
                idleSince = now;
            }
            if (now - idleSince < IDLE_GRACE_MILLIS) {
                return;
            }
            if (nextAttemptAt > now) {
                return;
            }
            if (lastSwitchEndAt > 0 && now - lastSwitchEndAt < MIN_SWITCH_GAP_MILLIS) {
                return;
            }
            /*
             * 切号条件：
             *   1) 当前账号这一轮的所有模块任务已经全部跑完（主条件）；
             *   2) 「切号等待间隔」> 0 时，还要满足驻留时长下限（可选，用户没要求时为 0）。
             *
             * 语义：「一个账号把所有操作做完 → 切到下一个账号 → 下一个做完 → 切回预设账号」。
             *
             * 「切号等待间隔」现在是**可选的驻留下限**：
             *   - 0（默认）：这一轮跑完就切，完全按用户期望的节奏走；
             *   - > 0：跑完之后还要在这个账号上至少待这么久才切，用于控制切号频率。
             *
             * 防连击靠的是 MIN_SWITCH_GAP_MILLIS（两次切号之间的硬性最小间隔），
             * 不再拿「执行间隔」兜底 —— 那会让账号白等一整个周期。
             */
            long intervalMillis = config.getAccountSwitchIntervalMinutes() * 60_000L;
            if (intervalMillis > 0) {
                /*
                 * residentSince 理论上由 onAccountSwitched / onRunResumed 写入。
                 * 但两处都要能拿到登录账号才写得上，启动早期或读不到账号时它一直是 0，
                 * 于是 residentMillis 恒为 0，驻留条件永远不满足 → 永不切号。
                 * 这里用运行起点兜底，保证「至少待了这么久」这个语义始终成立。
                 */
                long residentBase = residentSince > 0 ? residentSince : runStartTime;
                long residentMillis = residentBase > 0 ? now - residentBase : 0;
                if (residentMillis < intervalMillis) {
                    // 驻留时间还不够，任务虽已跑完也先不切
                    if (!intervalWaitLogged) {
                        intervalWaitLogged = true;
                        long remainMinutes = (intervalMillis - residentMillis + 59_999L) / 60_000L;
                        Log.record("自动切号🔄️ 当前账号任务已全部完成，" + label(currentHost.currentUid())
                                + " 已驻留 " + (residentMillis / 60_000L) + " 分钟，"
                                + "距离切号间隔还需 " + remainMinutes + " 分钟，等待中");
                    }
                    return;
                }
            }
            intervalWaitLogged = false;
            switching = true;
            try {
                switchExecutor.execute(() -> doSwitch(config));
            } catch (Throwable t) {
                // 提交失败必须复位，否则 switching 会一直为 true，后续所有切号检查都会被挡住
                Log.printStackTrace(TAG + "#提交切号失败", t);
                switching = false;
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
            }
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#自动切号检查失败", t);
        }
    }

    private static void doSwitch(AccountSwitchConfig.Snapshot config) {
        Host currentHost = host;
        boolean completed = false;
        boolean stoppedTasks = false;
        try {
            if (currentHost == null) {
                return;
            }
            // 用磁盘上的最新配置，避免切号过程中用户在设置里改了账号/顺序仍按旧快照执行
            config = AccountSwitchConfig.readFresh();
            if (!config.isAccountSwitchEnabled()) {
                Log.record("自动切号🔄️ 自动切换登录账号已关闭，取消本次切号");
                return;
            }
            if (scheduledSwitchAt > 0) {
                // 定时切号已接管，当前任务已终止，直接交给定时流程
                Log.record("自动切号🔄️ 定时切号已接管，取消本次切号");
                return;
            }
            String currentUid = AlipayAccountSwitchBridge.currentUid();
            if (StringUtil.isEmpty(currentUid)) {
                // 读不到当前账号时不能盲目切号，避免切到错误账号或把登录态切丢
                Log.record("自动切号🔄️ 读取当前登录账号失败，稍后重试");
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                return;
            }
            List<AlipayAccountSwitchBridge.HostAccount> hostAccounts =
                    AlipayAccountSwitchBridge.accounts();
            LinkedHashMap<String, AlipayAccountSwitchBridge.HostAccount> hostMap =
                    new LinkedHashMap<>();
            for (AlipayAccountSwitchBridge.HostAccount account : hostAccounts) {
                hostMap.put(account.uid, account);
            }
            List<AccountSwitchConfig.Account> ordered = config.getAccountSwitchAccounts();
            if (ordered.size() < 2) {
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                return;
            }

            int holdIndex = indexOf(ordered, config.getAccountSwitchFinalAccountUid());
            AccountSwitchConfig.Account holdAccount = holdIndex >= 0 ? ordered.get(holdIndex) : null;
            // 常态保持账号必须有登录记录，否则退回"回到列表第一个账号"的逻辑
            boolean holdUsable = holdIndex >= 0 && holdAccount != null
                    && !StringUtil.isEmpty(resolveLoginId(holdAccount, hostMap));

            /*
            /*
             * 切号顺序：**完全按配置里的账号顺序**。
             * <p>常态保持账号就待在它配置的位置，不把它挪到末尾 ——
             * 挪走会让访问顺序与用户配置不一致
             * （配置 [账号1, 账号3(常态), 账号5] 时会变成 账号1 → 账号5 → 账号3）。
             *
             * 一轮 = 从列表第一个账号起，依次访问后面的账号，绕回起点即走完；
             * 其中「当前账号就是常态保持账号、且所有账号都访问过了」的那一刻，这一轮收尾。
             */
            List<AccountSwitchConfig.Account> effective = ordered;
            if (effective.size() < 2) {
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                return;
            }
            int currentIndex = indexOf(effective, currentUid);
            // 常态保持账号在生效顺序里的下标：轮次收尾与下一轮起点都以它为基准
            // 目标起点：当前账号之后的下一个账号
            int start = currentIndex >= 0 ? currentIndex + 1 : 0;

            AccountSwitchConfig.Account target = null;
            int targetIndex = -1;
            for (int step = 0; step < effective.size(); step++) {
                int index = (start + step) % effective.size();
                AccountSwitchConfig.Account candidate = effective.get(index);
                if (candidate == null || StringUtil.isEmpty(candidate.getUid())) {
                    continue;
                }
                if (candidate.getUid().equals(currentUid)) {
                    continue;
                }
                if (StringUtil.isEmpty(resolveLoginId(candidate, hostMap))) {
                    // 该账号在支付宝本地已无登录记录，跳过
                    continue;
                }
                target = candidate;
                targetIndex = index;
                break;
            }

            if (target == null) {
                Log.record("自动切号🔄️ 没有可切换的账号，稍后重试");
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                return;
            }

            /*
            /*
             * 本轮是否走完：**当前账号就是常态保持账号**，且本轮已访问过所有可用账号。
             *
             * <p>注意判定的主体是「当前账号」而不是「切过去的那个目标」：
             * 常态保持账号要等它自己这一轮的任务跑完，这一轮才算收尾。
             * 早先按「目标是常态账号」判定，导致一轮走完时直接进入轮次等待、
             * **根本没执行切号** —— 账号停在最后跑完的那个账号上，
             * 表现就是「本轮结束了却没有切回常态保持账号」。
             *
             * <p>用「已访问集合」而不是按下标推算：无论从哪个账号启动、
             * 中途账号是否失效，访问进度都不会算错。
             */
            Set<String> available = new HashSet<>();
            for (AccountSwitchConfig.Account account : effective) {
                if (account == null || StringUtil.isEmpty(account.getUid())) {
                    continue;
                }
                if (!StringUtil.isEmpty(resolveLoginId(account, hostMap))) {
                    available.add(account.getUid());
                }
            }
            /*
             * 判定时把当前账号也算上：它这一轮的任务已经跑完，属于本轮已访问。
             * 不算上的话，最后一个账号永远凑不齐「已访问全部账号」，
             * 一轮永远走不完、轮次间隔永远不触发。
             */
            Set<String> visitedNow = new HashSet<>(roundVisited);
            visitedNow.add(currentUid);
            boolean roundEnd = holdUsable
                    && holdAccount != null
                    && holdAccount.getUid().equals(currentUid)
                    && visitedNow.containsAll(available);

            String loginId = resolveLoginId(target, hostMap);
            boolean sameAccount = target.getUid().equals(currentUid);
            if (!sameAccount && StringUtil.isEmpty(loginId)) {
                Log.record("自动切号🔄️ 没有可切换的账号，稍后重试");
                nextAttemptAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                return;
            }

            /*
             * 轮次等待：本轮所有账号都执行完之后，等「轮次间隔」再开始下一轮。
             * <p>计时起点是「本轮走完的那一刻」，不是切入第一个账号时 ——
             * 否则各账号自己跑任务的时间会被白白吃掉，轮次间隔形同虚设。
             *
             * <p>轮次间隔为 0 时回落到「执行间隔」（基础设置里的执行间隔，默认 50 分钟）：
             * 一轮走完就该歇一会儿再开始下一轮，否则只剩两次切号之间的硬性最小间隔（3 分钟），
             * 表现就是「切号 → 跑一轮 → 切号」的三分钟无限循环。
             */
            long roundIntervalMinutes = config.getAccountSwitchRoundIntervalMinutes();
            if (roundIntervalMinutes <= 0) {
                // checkInterval 的值是毫秒（MultiplyIntegerModelField 带 60000 倍率），换算成分钟
                roundIntervalMinutes = Math.max(BaseModel.getCheckInterval().getValue() / 60_000L, 1L);
            }
            /*
             * 本轮走完：立刻切回**常态保持账号**，然后等轮次间隔再开始下一轮。
             *
             * <p>切回常态账号这一步必须**立即执行**，不能推到轮次等待结束：
             * 之前是先等完轮次间隔才切，切过去后 initialize(uid, true) 会立刻重跑一轮，
             * 于是「切回账号1 → 账号1 又执行一次任务」，
             * 而用户期望的是切回常态账号后**停着**等间隔，间隔到了才开始下一轮。
             *
             * <p>所以这里：切号目标改成常态保持账号 → 切过去 → 不启动任务（startMain=false）
             * → 进入轮次等待 → 间隔到了再启动下一轮。
             */
            if (roundEnd) {
                // 本轮刚走完：记下这个时刻作为轮次间隔的计时起点
                if (roundFinishedAt <= 0) {
                    roundFinishedAt = System.currentTimeMillis();
                }
                // 切号目标改成常态保持账号
                int holdTargetIndex = holdIndex;
                if (holdTargetIndex >= 0 && holdTargetIndex != targetIndex) {
                    target = effective.get(holdTargetIndex);
                    targetIndex = holdTargetIndex;
                    loginId = resolveLoginId(target, hostMap);
                    sameAccount = target.getUid().equals(currentUid);
                }
            }
            Log.record("自动切号🔄️ 开始切换: " + label(currentUid) + " -> " + label(target)
                    + (roundEnd ? "（本轮所有账号已执行完，切回常态保持账号）" : ""));
            paused = true;
            stoppedTasks = true;
            currentHost.pause();
            // 等旧账号的任务线程真正退出再换登录态：
            // pause() 只是通知线程作废，线程要到下一个检查点才结束，
            // 不等它退出会让旧账号的收尾动作与新账号的任务重叠（两个账号同时处理同一个任务）
            if (!ModelTask.waitForAllTaskStopped(WAIT_TASK_STOPPED_MILLIS)) {
                Log.record("自动切号🔄️ 旧账号任务未在 " + (WAIT_TASK_STOPPED_MILLIS / 1000)
                        + " 秒内完全退出，仍继续切换");
            }

            if (!sameAccount) {
                if (!AlipayAccountSwitchBridge.switchTo(target.getUid(), loginId)) {
                    logSwitchRejected();
                    return;
                }
                if (!waitForAccount(target.getUid())) {
                    Log.record("自动切号🔄️ 等待账号切换超时");
                    return;
                }
            }

            if (scheduledSwitchAt <= 0) {
                paused = false;
            }
            // 当前账号这一轮的任务已跑完，计入本轮已访问
            roundVisited.add(currentUid);
            // 先记驻留起点，再初始化：初始化里会回调 onRunResumed，
            // 顺序反了会被当成「外部换了账号」，把驻留计时清零
            onAccountSwitched(target.getUid());
            /*
             * 一轮走完切回常态账号后**不要立刻跑任务**：
             * 常态账号先停着，等轮次间隔到了再开始下一轮。
             * 用 startMain=false 只加载配置、不启动任务。
             */
            completed = currentHost.initialize(target.getUid(), !roundEnd);
            if (!completed) {
                Log.record("自动切号🔄️ 切换后加载账号失败");
            }
            Log.record("自动切号🔄️ 已切换到账号: " + label(target)
                    + (roundEnd ? "（本轮所有账号已执行完）" : ""));
            if (roundEnd) {
                // 本轮走完：清空访问记录，进入轮次等待，间隔到了再开始下一轮
                roundVisited.clear();
                long waitRemainMillis = roundFinishedAt + roundIntervalMinutes * 60_000L
                        - System.currentTimeMillis();
                if (waitRemainMillis > 0) {
                    awaitingResume = true;
                    pendingSwitchUid = firstAvailableUid(effective, hostMap);
                    roundWaitLastLogAt = System.currentTimeMillis();
                    resumeAt = System.currentTimeMillis() + waitRemainMillis;
                    Log.record("自动切号🔄️ 本轮所有账号已执行完，已切回常态保持账号 "
                            + label(target) + "，等待约 "
                            + ((waitRemainMillis + 59_999L) / 60_000L) + " 分钟后开始下一轮");
                }
            }
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#切换账号失败", t);
        } finally {
            if (!completed && stoppedTasks && host != null) {
                if (scheduledSwitchAt > 0) {
                    // 定时切号已接管，保持任务停止，等定时流程处理
                    paused = true;
                } else {
                    // 切号失败：恢复当前账号运行，避免任务一直停着
                    awaitingResume = false;
                    pendingSwitchUid = null;
                    paused = false;
                    try {
                        host.resume();
                    } catch (Throwable t) {
                        Log.printStackTrace(TAG + "#恢复任务失败", t);
                    }
                    // resume 会重置 nextAttemptAt，这里要放在后面设置才能生效
                    nextAttemptAt = System.currentTimeMillis() + failedRetryMillis();
                }
            }
            if (stoppedTasks) {
                lastSwitchEndAt = System.currentTimeMillis();
            }
            switching = false;
        }
    }

    /**
     * 轮次等待结束后执行：从保持账号切换到下一轮账号，并立即开始执行任务。
     */
    private static void resumeSwitch(String targetUid) {
        Host currentHost = host;
        boolean completed = false;
        boolean stoppedTasks = false;
        try {
            if (currentHost == null) {
                return;
            }
            // 等待期间配置可能已变，按磁盘最新内容确认开关与登录记录
            AccountSwitchConfig.Snapshot config = AccountSwitchConfig.readFresh();
            if (!config.isAccountSwitchEnabled()) {
                Log.record("自动切号🔄️ 自动切换登录账号已关闭，取消本次切号");
                return;
            }
            if (scheduledSwitchAt > 0) {
                Log.record("自动切号🔄️ 定时切号已接管，取消本次轮次切号");
                return;
            }
            String loginId = null;
            for (AlipayAccountSwitchBridge.HostAccount account : AlipayAccountSwitchBridge.accounts()) {
                if (account != null && targetUid.equals(account.uid)) {
                    loginId = account.loginId;
                    break;
                }
            }
            if (StringUtil.isEmpty(loginId)) {
                Log.record("自动切号🔄️ 目标账号已无登录记录，取消本次切号");
                return;
            }
            Log.record("自动切号🔄️ 轮次等待结束，开始切换: "
                    + label(currentHost.currentUid()) + " -> " + label(targetUid));
            paused = true;
            stoppedTasks = true;
            currentHost.pause();
            // 等旧账号任务真正退出，避免两个账号的任务重叠
            if (!ModelTask.waitForAllTaskStopped(WAIT_TASK_STOPPED_MILLIS)) {
                Log.record("自动切号🔄️ 旧账号任务未在 " + (WAIT_TASK_STOPPED_MILLIS / 1000)
                        + " 秒内完全退出，仍继续切换");
            }
            if (!AlipayAccountSwitchBridge.switchTo(targetUid, loginId)) {
                logSwitchRejected();
                return;
            }
            if (!waitForAccount(targetUid)) {
                Log.record("自动切号🔄️ 等待账号切换超时");
                return;
            }
            if (scheduledSwitchAt <= 0) {
                paused = false;
            }
            onAccountSwitched(targetUid);
            completed = currentHost.initialize(targetUid, true);
            if (completed) {
                // 新一轮开始：清空访问记录
                roundVisited.clear();
                roundFinishedAt = 0L;
                Log.record("自动切号🔄️ 已切换到账号: " + label(targetUid) + "，开始新一轮");
            } else {
                Log.record("自动切号🔄️ 切换后加载账号失败");
            }
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#轮次切号失败", t);
        } finally {
            if (!completed && host != null) {
                if (scheduledSwitchAt > 0) {
                    // 定时切号已接管，保持任务停止
                    paused = true;
                } else {
                    // 切号失败/取消：恢复当前保持账号运行，避免任务一直停着
                    paused = false;
                    try {
                        host.resume();
                    } catch (Throwable t) {
                        Log.printStackTrace(TAG + "#恢复任务失败", t);
                    }
                    /*
                     * 失败后不能就这么算了：轮次等待已经结束、resumeAt 也过期了，
                     * 若只恢复运行就再也没人触发这次切换，下一轮会被整个跳过。
                     * 这里把等待重新排上，等 failedRetryMillis 后再试一次。
                     *
                     * pendingSwitchUid 在进入 resumeSwitch 前被清空了，
                     * 这里从账号配置里还原目标 —— 它就是这一轮要切过去的账号。
                     */
                    long retryDelay = failedRetryMillis();
                    awaitingResume = true;
                    resumeAt = System.currentTimeMillis() + retryDelay;
                    roundWaitLastLogAt = System.currentTimeMillis();
                    if (StringUtil.isEmpty(pendingSwitchUid)) {
                        // 重试目标：账号列表里的第一个账号（登录记录在重试时再校验）
                        // 重试目标：账号列表里的第一个账号（登录记录在重试时再校验）
                        for (AccountSwitchConfig.Account account :
                                AccountSwitchConfig.getAccountSwitchAccounts()) {
                            if (account != null && !StringUtil.isEmpty(account.getUid())) {
                                pendingSwitchUid = account.getUid();
                                break;
                            }
                        }
                    }
                    Log.record("自动切号🔄️ 轮次切号失败，" + (retryDelay / 60_000L)
                            + " 分钟后重试");
                }
            }
            if (stoppedTasks) {
                lastSwitchEndAt = System.currentTimeMillis();
            }
            switching = false;
        }
    }

    /**
     * 定时切号第一步：不管当前处于什么状态，立即终止当前账号任务，
     * 终止完成后固定等待 20 秒，再切到保持账号重新执行。
     */
    private static void beginScheduledSwitch(Host currentHost) {
        paused = true;
        awaitingResume = false;
        pendingSwitchUid = null;
        roundWaitLastLogAt = 0;
        if (currentHost != null) {
            try {
                currentHost.pause();
            } catch (Throwable t) {
                Log.printStackTrace(TAG + "#定时切号终止任务失败", t);
            }
        }
        // 每次到点都重新计时，若上一次定时切号还在执行，直接以本次为准
        scheduledRetryCount = 0;
        scheduledSwitchAt = System.currentTimeMillis() + SCHEDULE_SWITCH_DELAY_MILLIS;
        Log.record("自动切号🔄️ 定时切号时间到，已终止当前账号任务，20 秒后切换到保持账号");
    }

    /**
     * 判断是否有「切换账号定时执行」时间点落在上一次检查到现在之间。
     * 命中后记录该时间点，避免同一天同一时间重复触发；进程刚启动时只记录起点，不补触发历史时间点。
     */
    private static boolean checkScheduleTrigger(AccountSwitchConfig.Snapshot config, long now) {
        long previous = lastScheduleCheckAt;
        lastScheduleCheckAt = now;
        List<String> times = config.getAccountSwitchScheduleTimes();
        if (times.isEmpty() || previous <= 0 || now <= previous) {
            return false;
        }
        long todayStart = TimeUtil.getToday().getTimeInMillis();
        if (todayStart != scheduleDayStart) {
            scheduleDayStart = todayStart;
            firedSchedulePoints.clear();
        }
        for (String time : times) {
            Calendar calendar = TimeUtil.getCalendarByTimeStr(now, time);
            if (calendar == null) {
                continue;
            }
            long scheduledAt = calendar.getTimeInMillis();
            if (scheduledAt <= previous || scheduledAt > now) {
                continue;
            }
            if (firedSchedulePoints.add(todayStart + "|" + time)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 定时切号：任务已终止且等待结束后，切换到常态保持账号并立即执行一轮；
     * 未配置可用的保持账号时，退回列表第一个可用账号。
     */
    private static void switchToHoldAndRun() {
        Host currentHost = host;
        boolean completed = false;
        boolean retryable = false;
        try {
            if (currentHost == null) {
                return;
            }
            AccountSwitchConfig.Snapshot config = AccountSwitchConfig.readFresh();
            if (!config.isAccountSwitchEnabled()) {
                Log.record("自动切号🔄️ 自动切换登录账号已关闭，取消定时切号");
                return;
            }
            String currentUid = AlipayAccountSwitchBridge.currentUid();
            if (StringUtil.isEmpty(currentUid)) {
                Log.record("自动切号🔄️ 读取当前登录账号失败，取消定时切号");
                return;
            }
            LinkedHashMap<String, AlipayAccountSwitchBridge.HostAccount> hostMap =
                    new LinkedHashMap<>();
            for (AlipayAccountSwitchBridge.HostAccount account : AlipayAccountSwitchBridge.accounts()) {
                if (account != null) {
                    hostMap.put(account.uid, account);
                }
            }
            List<AccountSwitchConfig.Account> ordered = config.getAccountSwitchAccounts();
            AccountSwitchConfig.Account target = null;
            int holdIndex = indexOf(ordered, config.getAccountSwitchFinalAccountUid());
            if (holdIndex >= 0
                    && !StringUtil.isEmpty(resolveLoginId(ordered.get(holdIndex), hostMap))) {
                target = ordered.get(holdIndex);
            } else {
                for (AccountSwitchConfig.Account account : ordered) {
                    if (!StringUtil.isEmpty(resolveLoginId(account, hostMap))) {
                        target = account;
                        break;
                    }
                }
            }
            if (target == null) {
                Log.record("自动切号🔄️ 定时切号未找到可切换的账号");
                return;
            }
            final String targetUid = target.getUid();
            final String loginId = resolveLoginId(target, hostMap);
            boolean sameAccount = targetUid.equals(currentUid);
            retryable = true;
            Log.record("自动切号🔄️ 定时切号开始: " + label(currentUid) + " -> " + label(targetUid));
            if (!sameAccount) {
                paused = true;
                currentHost.pause();
                // 等旧账号任务真正退出，避免两个账号的任务重叠
                if (!ModelTask.waitForAllTaskStopped(WAIT_TASK_STOPPED_MILLIS)) {
                    Log.record("自动切号🔄️ 旧账号任务未在 " + (WAIT_TASK_STOPPED_MILLIS / 1000)
                            + " 秒内完全退出，仍继续切换");
                }
                if (!AlipayAccountSwitchBridge.switchTo(targetUid, loginId)) {
                    logSwitchRejected();
                    return;
                }
                if (!waitForAccount(targetUid)) {
                    Log.record("自动切号🔄️ 等待账号切换超时");
                    return;
                }
            }
            if (scheduledSwitchAt > 0) {
                // 执行期间又有新的定时点到达：放弃本次，等新的定时流程重新执行
                Log.record("自动切号🔄️ 新的定时切号已接管，取消本次切换");
                return;
            }
            paused = false;
            /*
             * 清空本轮访问记录：定时切号后这一轮会从常态保持账号出发，
             * 依次走完其余账号再绕回来 —— 一轮里所有账号都执行到，
             * 而不是只跑常态保持账号一个就结束。
             */
            roundVisited.clear();
            roundFinishedAt = 0L;
            onAccountSwitched(targetUid);
            completed = currentHost.initialize(targetUid, true);
            if (completed) {
                Log.record("自动切号🔄️ 定时切号完成，已切换到 " + label(targetUid)
                        + "，本轮将依次执行其余账号");
            } else {
                Log.record("自动切号🔄️ 切换后加载账号失败");
            }
        } catch (Throwable t) {
            Log.printStackTrace(TAG + "#定时切号失败", t);
        } finally {
            if (!completed && host != null) {
                if (scheduledSwitchAt > 0) {
                    // 新的定时切号已接管，保持任务停止
                    paused = true;
                } else {
                    // 取消/失败：先恢复当前账号运行，避免任务一直停着
                    paused = false;
                    try {
                        host.resume();
                    } catch (Throwable t) {
                        Log.printStackTrace(TAG + "#恢复任务失败", t);
                    }
                    nextAttemptAt = System.currentTimeMillis() + failedRetryMillis();
                    // 定时切号是硬需求：失败后自动重试，避免当天这个时间点直接作废
                    if (retryable && scheduledRetryCount < SCHEDULED_MAX_RETRY) {
                        scheduledRetryCount++;
                        scheduledSwitchAt = System.currentTimeMillis() + FAILED_RETRY_MILLIS;
                        Log.record("自动切号🔄️ 定时切号失败，60 秒后重试（第 "
                                + scheduledRetryCount + " 次）");
                    } else if (retryable) {
                        scheduledRetryCount = 0;
                        Log.record("自动切号🔄️ 定时切号重试次数已达上限，放弃本次");
                    }
                }
            } else if (completed) {
                scheduledRetryCount = 0;
            }
            lastSwitchEndAt = System.currentTimeMillis();
            switching = false;
        }
    }

    private static boolean waitForAccount(String targetUid) {
        for (int i = 0; i < SWITCH_POLL_COUNT; i++) {
            try {
                Thread.sleep(SWITCH_POLL_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            String currentUid = AlipayAccountSwitchBridge.currentUid();
            if (targetUid.equals(currentUid)) {
                return true;
            }
        }
        return false;
    }

    private static String resolveLoginId(AccountSwitchConfig.Account account,
                                         Map<String, AlipayAccountSwitchBridge.HostAccount> hostMap) {
        if (account == null) {
            return null;
        }
        AlipayAccountSwitchBridge.HostAccount hostAccount = hostMap.get(account.getUid());
        if (hostAccount != null && !StringUtil.isEmpty(hostAccount.loginId)) {
            return hostAccount.loginId;
        }
        return account.getLoginId();
    }

    private static int indexOf(List<AccountSwitchConfig.Account> accounts, String uid) {
        if (StringUtil.isEmpty(uid)) {
            return -1;
        }
        for (int i = 0; i < accounts.size(); i++) {
            AccountSwitchConfig.Account account = accounts.get(i);
            if (account != null && uid.equals(account.getUid())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 列表里第一个可用的账号 uid。
     * <p>「可用」= 在支付宝本地还有登录记录。取不到返回 null。
     */
    private static String firstAvailableUid(List<AccountSwitchConfig.Account> effective,
                                             Map<String, AlipayAccountSwitchBridge.HostAccount> hostMap) {
        if (effective == null) {
            return null;
        }
        for (AccountSwitchConfig.Account candidate : effective) {
            if (candidate == null || StringUtil.isEmpty(candidate.getUid())) {
                continue;
            }
            if (!StringUtil.isEmpty(resolveLoginId(candidate, hostMap))) {
                return candidate.getUid();
            }
        }
        return null;
    }

    /**
     * 下一轮的起点账号：给定下标之后的第一个可用账号（循环查找）。
     * <p>用于「常态保持账号收尾后，紧接着切到下一个账号继续这一轮」。
     */
    private static String resolveNextRoundUid(List<AccountSwitchConfig.Account> effective,
                                              int fromIndex,
                                              Map<String, AlipayAccountSwitchBridge.HostAccount> hostMap,
                                              String currentUid) {
        if (effective == null || effective.isEmpty()) {
            return currentUid;
        }
        for (int step = 1; step <= effective.size(); step++) {
            int index = (fromIndex + step) % effective.size();
            AccountSwitchConfig.Account candidate = effective.get(index);
            if (candidate == null || StringUtil.isEmpty(candidate.getUid())) {
                continue;
            }
            if (!StringUtil.isEmpty(resolveLoginId(candidate, hostMap))) {
                return candidate.getUid();
            }
        }
        return currentUid;
    }

    /** 切号被拒时的日志：1014 说明目标账号凭证失效，需要用户手动登录一次 */
    private static void logSwitchRejected() {
        if (AlipayAccountSwitchBridge.isManualLoginRequired()) {
            Log.record("自动切号🔄️ 支付宝要求重新输入账号密码，本次切换跳过，请手动登录该账号一次");
        } else {
            Log.record("自动切号🔄️ 支付宝未受理切号请求");
        }
    }

    /** 失败重试间隔：需要重新登录时拉长间隔，避免无意义地反复尝试 */
    private static long failedRetryMillis() {
        return AlipayAccountSwitchBridge.isManualLoginRequired()
                ? MANUAL_LOGIN_RETRY_MILLIS : FAILED_RETRY_MILLIS;
    }

    private static boolean isSameAccount(AccountSwitchConfig.Account left,
                                         AccountSwitchConfig.Account right) {
        return left != null && right != null
                && !StringUtil.isEmpty(left.getUid())
                && left.getUid().equals(right.getUid());
    }

    /** 日志统一用登录名(loginId)显示账号，查不到再退回 uid */
    private static String label(String uid) {
        if (StringUtil.isEmpty(uid)) {
            return "未知";
        }
        for (AccountSwitchConfig.Account account : AccountSwitchConfig.getAccountSwitchAccounts()) {
            if (account != null && uid.equals(account.getUid())) {
                if (!StringUtil.isEmpty(account.getLoginId())) {
                    return account.getLoginId();
                }
                break;
            }
        }
        return uid;
    }

    private static String label(AccountSwitchConfig.Account account) {
        if (account == null) {
            return "未知";
        }
        if (!StringUtil.isEmpty(account.getLoginId())) {
            return account.getLoginId();
        }
        if (!StringUtil.isEmpty(account.getName())) {
            return account.getName();
        }
        return StringUtil.isEmpty(account.getUid()) ? "未知" : account.getUid();
    }

    private static final class DaemonThreadFactory implements ThreadFactory {

        private int index;

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable, "AccountSwitch-" + (++index));
            thread.setDaemon(true);
            return thread;
        }
    }
}
