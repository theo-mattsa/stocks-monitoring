package com.soe.ui;

import java.io.IOException;
import java.text.NumberFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;

import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.DefaultWindowManager;
import com.googlecode.lanterna.gui2.Direction;
import com.googlecode.lanterna.gui2.EmptySpace;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.screen.TerminalScreen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.Terminal;

public class DashboardUI {
    private MultiWindowTextGUI gui;
    private Table<String> quoteTable;
    private TextBox eventLog;
    private Label marketStatus;
    private Label symbolCount;
    private Label eventCount;
    private int totalEvents = 0;
    private static DashboardUI instance;
    private final NumberFormat priceFormat;
    private final NumberFormat percentFormat;
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private DashboardUI() {
        priceFormat = NumberFormat.getNumberInstance(Locale.US);
        priceFormat.setMinimumFractionDigits(4);
        priceFormat.setMaximumFractionDigits(4);

        percentFormat = NumberFormat.getPercentInstance(Locale.US);
        percentFormat.setMinimumFractionDigits(0);
        percentFormat.setMaximumFractionDigits(4);
    }

    public static DashboardUI getInstance() {
        if (instance == null) instance = new DashboardUI();
        return instance;
    }

    public static boolean isGuiInitialized() {
        return instance != null && instance.gui != null;
    }

    public void start() {
        new Thread(() -> {
            try {
                Terminal terminal = new DefaultTerminalFactory().createTerminal();
                Screen screen = new TerminalScreen(terminal);
                screen.startScreen();

                Panel mainPanel = new Panel();
                mainPanel.setLayoutManager(new LinearLayout(Direction.VERTICAL));
                mainPanel.setPreferredSize(new TerminalSize(80, 30));

                Label title = new Label(" STOCKS MONITORING SYSTEM ");
                title.setForegroundColor(TextColor.ANSI.CYAN);
                mainPanel.addComponent(title);

                Label subtitle = new Label(" Real-Time Market Dashboard");
                subtitle.setForegroundColor(TextColor.ANSI.WHITE);
                mainPanel.addComponent(subtitle);

                mainPanel.addComponent(new EmptySpace(new TerminalSize(0, 1)));

                Panel statusPanel = new Panel();
                statusPanel.setLayoutManager(new LinearLayout(Direction.HORIZONTAL));

                marketStatus = new Label(" ● MARKET: LIVE ");
                marketStatus.setForegroundColor(TextColor.ANSI.GREEN);

                symbolCount = new Label(" │ SYMBOLS: 0 ");
                symbolCount.setForegroundColor(TextColor.ANSI.YELLOW);

                eventCount = new Label(" │ EVENTS: 0 ");
                eventCount.setForegroundColor(TextColor.ANSI.YELLOW);

                statusPanel.addComponent(marketStatus);
                statusPanel.addComponent(symbolCount);
                statusPanel.addComponent(eventCount);
                mainPanel.addComponent(statusPanel);

                mainPanel.addComponent(new EmptySpace(new TerminalSize(0, 1)));

                Label quoteTitle = new Label("┌────────────── LIVE QUOTES ──────────────┐");
                quoteTitle.setForegroundColor(TextColor.ANSI.CYAN);
                mainPanel.addComponent(quoteTitle);

                quoteTable = new Table<>("ATIVO", "PREÇO", "VARIAÇÃO");
                quoteTable.setPreferredSize(new TerminalSize(70, 8));
                mainPanel.addComponent(quoteTable);

                Label quoteBottom = new Label("└─────────────────────────────────────────┘");
                quoteBottom.setForegroundColor(TextColor.ANSI.CYAN);
                mainPanel.addComponent(quoteBottom);

                mainPanel.addComponent(new EmptySpace(new TerminalSize(0, 1)));

                Label eventTitle = new Label("┌────────────── MARKET EVENTS ────────────┐");
                eventTitle.setForegroundColor(TextColor.ANSI.CYAN);
                mainPanel.addComponent(eventTitle);

                eventLog = new TextBox("", TextBox.Style.MULTI_LINE);
                eventLog.setPreferredSize(new TerminalSize(70, 8));
                eventLog.setReadOnly(true);
                mainPanel.addComponent(eventLog);

                Label eventBottom = new Label("└─────────────────────────────────────────┘");
                eventBottom.setForegroundColor(TextColor.ANSI.CYAN);
                mainPanel.addComponent(eventBottom);

                BasicWindow window = new BasicWindow(" Stocks Monitoring System ");
                window.setComponent(mainPanel);
                window.setHints(Arrays.asList(Window.Hint.CENTERED));

                gui = new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(TextColor.ANSI.BLACK));
                gui.addWindowAndWait(window);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "ui-thread").start();
    }

    public void updateQuote(String symbol, Double price, Double change) {
        if (gui == null) return;

        gui.getGUIThread().invokeLater(() -> {
            int rowCount = quoteTable.getTableModel().getRowCount();

            for (int i = 0; i < rowCount; i++) {
                if (quoteTable.getTableModel().getRow(i).get(0).equals(symbol)) {
                    quoteTable.getTableModel().removeRow(i);
                    break;
                }
            }

            String formattedPrice = priceFormat.format(price);
            String formattedChange = percentFormat.format(change);

            if (change > 0) formattedChange = "+" + formattedChange;

            quoteTable.getTableModel().addRow(symbol, formattedPrice, formattedChange);

            symbolCount.setText(" │ SYMBOLS: " + quoteTable.getTableModel().getRowCount() + " ");
        });
    }

    public void logEvent(String eventMessage) {
        if (gui == null) return;

        gui.getGUIThread().invokeLater(() -> {
            totalEvents++;

            String timestamp = LocalTime.now().format(timeFormatter);
            String newEvent = "[" + timestamp + "] " + eventMessage;
            String newText = newEvent + "\n" + eventLog.getText();

            eventLog.setText(newText);
            eventCount.setText(" │ EVENTS: " + totalEvents + " ");
        });
    }

    public void setMarketStatus(boolean connected) {
        if (gui == null) return;

        gui.getGUIThread().invokeLater(() -> {
            if (connected) {
                marketStatus.setText(" ● MARKET: LIVE ");
                marketStatus.setForegroundColor(TextColor.ANSI.GREEN);
            } else {
                marketStatus.setText(" ● MARKET: OFFLINE ");
                marketStatus.setForegroundColor(TextColor.ANSI.RED);
            }
        });
    }
}