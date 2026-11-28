package io.github.aw1y2z.sesame.data;

import java.util.ArrayList;

import lombok.Getter;

import io.github.aw1y2z.sesame.data.modelFieldExt.AccountSwitchBooleanModelField;
import io.github.aw1y2z.sesame.data.modelFieldExt.AccountSwitchFinalAccountModelField;
import io.github.aw1y2z.sesame.data.modelFieldExt.AccountSwitchIntervalModelField;
import io.github.aw1y2z.sesame.data.modelFieldExt.AccountSwitchListModelField;
import io.github.aw1y2z.sesame.data.modelFieldExt.AccountSwitchTimesModelField;
import io.github.aw1y2z.sesame.data.modelFieldExt.FeatureFlagBooleanModelField;

/**
 * 多账号切换相关的全局配置字段集合。
 *
 * <p>这些字段的值统一读写 config/config_common.json，不跟随任何登录账号，
 * 因此**不属于任何账号的独立配置**：统一由「多账号切换」设置页渲染一次，
 * 不再注册到 {@code BaseModel}（基础分组）里 —— 否则每个账号的独立设置里
 * 都会出现一份内容完全相同的切号设置，改哪一份都会让人困惑。
 *
 * <p>字段实例是进程级单例，界面改动统一走 {@link GlobalConfigField#applyGlobalValue(Object)}，
 * 由它把新值写进公共配置文件。
 */
public final class AccountSwitchFields {

    @Getter
    private static final AccountSwitchBooleanModelField accountSwitchEnabled =
            new AccountSwitchBooleanModelField("accountSwitchEnabled", "自动切换登录账号", false)
                    .setDescription("开启后按下面的顺序循环切换支付宝登录账号；账号列表在模块启动时自动读取");

    @Getter
    private static final AccountSwitchIntervalModelField accountSwitchIntervalMinutes =
            new AccountSwitchIntervalModelField("accountSwitchIntervalMinutes",
                    "切号等待间隔(分钟,0跑完就切)", 0, 0, 24 * 60,
                    AccountSwitchIntervalModelField.KIND_SWITCH)
                    .setDescription("当前账号这一轮所有模块操作完成后，再等这么久才切到下一个账号；0 表示跑完立即切换（轮次节奏由「切号轮次间隔」控制）");

    @Getter
    private static final AccountSwitchIntervalModelField accountSwitchRoundIntervalMinutes =
            new AccountSwitchIntervalModelField("accountSwitchRoundIntervalMinutes",
                    "切号轮次间隔(分钟,0按执行间隔)", 0, 0, 24 * 60,
                    AccountSwitchIntervalModelField.KIND_ROUND)
                    .setDescription("所有账号轮流执行完一整轮后，等待这么久才开始下一轮；0 表示按「执行间隔」计算（默认 50 分钟）");

    @Getter
    private static final AccountSwitchListModelField accountSwitchAccounts =
            new AccountSwitchListModelField("accountSwitchAccounts", "账号切换顺序列表", new ArrayList<>())
                    .setDescription("点击进入可上下移动调整切号顺序");

    @Getter
    private static final AccountSwitchFinalAccountModelField accountSwitchFinalAccountUid =
            new AccountSwitchFinalAccountModelField("accountSwitchFinalAccountUid", "常态保持账号(轮完切入)")
                    .setDescription("每一轮所有账号轮流执行完后切到该账号作为收尾，等待「切号轮次间隔」再开始下一轮；不选则按列表顺序循环");

    @Getter
    private static final AccountSwitchTimesModelField accountSwitchScheduleTimes =
            new AccountSwitchTimesModelField("accountSwitchScheduleTimes", "切换账号定时执行(如0800,2000)", new ArrayList<>())
                    .setDescription("到点后立即终止当前账号任务，20 秒后切到常态保持账号，并把这一轮的所有账号依次执行完；支持 HH、HHmm、HHmmss，逗号分隔");

    @Getter
    private static final FeatureFlagBooleanModelField autoSlideCaptchaEnabled =
            new FeatureFlagBooleanModelField("autoSlideCaptchaEnabled", "自动处理滑动/拼图验证", true,
                    FeatureFlagBooleanModelField.Flag.AUTO_SLIDE_CAPTCHA)
                    .setDescription("开启后自动识别并完成滑动与拼图验证，低版本走滑动手势，新版本走拼图截图识别");

    @Getter
    private static final FeatureFlagBooleanModelField unlockAccountLimitEnabled =
            new FeatureFlagBooleanModelField("unlockAccountLimitEnabled", "解除登录账号数量限制", true,
                    FeatureFlagBooleanModelField.Flag.UNLOCK_ACCOUNT_LIMIT)
                    .setDescription("开启后保留本机全部登录账号记录，不再被账号管理页清理到 5 个；不解除服务端风控限制");

    private AccountSwitchFields() {
    }

    /**
     * 多账号切换的全部全局字段，按界面展示顺序。
     * <p>不在 {@code BaseModel} 里注册，因此不会出现在任何账号的「基础」分组中。
     */
    public static ModelFields getFields() {
        ModelFields modelFields = new ModelFields();
        modelFields.addField(accountSwitchEnabled);
        modelFields.addField(accountSwitchIntervalMinutes);
        modelFields.addField(accountSwitchRoundIntervalMinutes);
        modelFields.addField(accountSwitchAccounts);
        modelFields.addField(accountSwitchFinalAccountUid);
        modelFields.addField(accountSwitchScheduleTimes);
        modelFields.addField(autoSlideCaptchaEnabled);
        modelFields.addField(unlockAccountLimitEnabled);
        return modelFields;
    }
}
