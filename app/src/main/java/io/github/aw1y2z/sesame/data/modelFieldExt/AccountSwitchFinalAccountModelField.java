package io.github.aw1y2z.sesame.data.modelFieldExt;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

import io.github.aw1y2z.sesame.data.AccountSwitchConfig;
import io.github.aw1y2z.sesame.data.GlobalConfigField;
import io.github.aw1y2z.sesame.util.Log;
import io.github.aw1y2z.sesame.util.StringUtil;

/**
 * 常态保持账号（单选，全局配置）。
 * <p>选项动态取自 config/config_common.json 的账号列表，选中项也保存到该文件。
 * 一轮账号全部切完后，切换到该账号等待「切号轮次间隔」，再开始下一轮。
 */
public class AccountSwitchFinalAccountModelField extends ChoiceModelField implements GlobalConfigField {

    /** 界面加载时的账号 uid 顺序，用于把单选下标还原成 uid（避免保存时列表顺序刚被调整而选错账号） */
    private volatile List<String> optionUids = new ArrayList<>();

    public AccountSwitchFinalAccountModelField(String code, String name) {
        super(code, name, 0, new String[]{"暂无账号"});
    }

    @Override
    @JsonIgnore
    public String[] getExpandKey() {
        List<AccountSwitchConfig.Account> accounts = AccountSwitchConfig.getAccountSwitchAccounts();
        if (accounts.isEmpty()) {
            optionUids = new ArrayList<>();
            return new String[]{"暂无账号"};
        }
        String[] keys = new String[accounts.size()];
        List<String> uids = new ArrayList<>(accounts.size());
        for (int i = 0; i < accounts.size(); i++) {
            keys[i] = accounts.get(i).getDisplayName();
            uids.add(accounts.get(i).getUid());
        }
        optionUids = uids;
        return keys;
    }

    @Override
    public void clampValue() {
        // 选项是动态的（账号列表会变），不能按构造时的固定选项数组判定越界：
        // 下标由 selectedIndex() 自己保证落在当前账号列表范围内
    }

    @Override
    public Integer getValue() {
        return selectedIndex();
    }

    @Override
    @JsonIgnore
    public String getConfigValue() {
        // 用 uid 而不是下标：账号列表会变（新增/删除账号、支付宝侧登录了新账号），
        // 下标会指向别的账号，常态保持账号就悄悄变成了另一个账号。
        String uid = AccountSwitchConfig.getAccountSwitchFinalAccountUid();
        return StringUtil.isEmpty(uid) ? "" : uid;
    }

    @Override
    @JsonIgnore
    public void setConfigValue(String configValue) {
        if (configValue == null) {
            return;
        }
        try {
            applyGlobalValue(configValue.trim());
        } catch (Throwable t) {
            Log.printStackTrace("AccountSwitchFinalAccountModelField#保存常态保持账号失败", t);
        }
    }

    /**
     * 只更新内存副本，不落盘：公共配置文件的写入只发生在 {@link #applyGlobalValue(Object)}，
     * 避免字段构造时（模型初始化）就把默认值写进文件、覆盖用户设置。
     */
    @Override
    public void setObjectValue(Object objectValue) {
        super.setObjectValue(selectedIndex());
    }

    @Override
    public void applyGlobalValue(Object value) {
        if (value == null) {
            super.setObjectValue(selectedIndex());
            return;
        }
        String raw = String.valueOf(value).trim();
        if (StringUtil.isEmpty(raw)) {
            super.setObjectValue(selectedIndex());
            return;
        }
        /*
         * 值是账号 uid，直接按 uid 保存 —— 账号列表变化不会让常态保持账号指错人。
         * 兼容旧数据：老版本存的是下标，若这个「uid」在账号列表里找不到，
         * 但它又是个纯数字，就当作旧下标处理（按当前列表重新定位）。
         */
        List<AccountSwitchConfig.Account> accounts =
                AccountSwitchConfig.getAccountSwitchAccounts();
        boolean found = false;
        for (AccountSwitchConfig.Account account : accounts) {
            if (account != null && raw.equals(account.getUid())) {
                found = true;
                break;
            }
        }
        if (!found && raw.chars().allMatch(Character::isDigit)) {
            try {
                int index = Integer.parseInt(raw);
                if (index >= 0 && index < accounts.size()
                        && accounts.get(index) != null) {
                    AccountSwitchConfig.setAccountSwitchFinalAccountUid(
                            accounts.get(index).getUid());
                    super.setObjectValue(selectedIndex());
                    return;
                }
            } catch (Exception ignored) {
                // 不是合法下标，按 uid 处理
            }
        }
        if (found) {
            AccountSwitchConfig.setAccountSwitchFinalAccountUid(raw);
        }
        super.setObjectValue(selectedIndex());
    }

    private int selectedIndex() {
        List<AccountSwitchConfig.Account> accounts = AccountSwitchConfig.getAccountSwitchAccounts();
        if (accounts.isEmpty()) {
            return 0;
        }
        String uid = AccountSwitchConfig.getAccountSwitchFinalAccountUid();
        if (!StringUtil.isEmpty(uid)) {
            for (int i = 0; i < accounts.size(); i++) {
                AccountSwitchConfig.Account account = accounts.get(i);
                if (account != null && uid.equals(account.getUid())) {
                    return i;
                }
            }
        }
        return 0;
    }
}
