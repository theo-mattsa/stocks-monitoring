package com.soe.ui;

import java.io.IOException;
import java.util.Arrays;

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

    public void start() {
        new Thread(() -> {
            try {
                Terminal terminal = new DefaultTerminalFactory().createTerminal();
                Screen screen = new TerminalScreen(terminal);
                screen.startScreen();

                // Main layout panel
                Panel mainPanel = new Panel();
                mainPanel.setLayoutManager(new LinearLayout(Direction.VERTICAL));

                // Quote table
                mainPanel.addComponent(new Label("--- Cotações ao Vivo ---"));
                quoteTable = new Table<>("Ativo", "Preço", "Variação");
                quoteTable.setPreferredSize(new TerminalSize(50, 5));
                mainPanel.addComponent(quoteTable);

                mainPanel.addComponent(new EmptySpace(new TerminalSize(0, 1)));

                // Events log
                mainPanel.addComponent(new Label("--- Eventos de Mercado ---"));
                eventLog = new TextBox("", TextBox.Style.MULTI_LINE);
                eventLog.setPreferredSize(new TerminalSize(50, 10));
                eventLog.setReadOnly(true);
                mainPanel.addComponent(eventLog);

                // Configure and display the window
                BasicWindow window = new BasicWindow("Stocks Monitoring System");
                window.setComponent(mainPanel);
                window.setHints(Arrays.asList(Window.Hint.CENTERED));

                gui = new MultiWindowTextGUI(screen, new DefaultWindowManager(), new EmptySpace(com.googlecode.lanterna.TextColor.ANSI.BLUE));
                gui.addWindowAndWait(window);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }, "ui-thread").start();
    }

    public void updateQuote(String symbol, String price, String change) {
        if (gui == null) return;
        gui.getGUIThread().invokeLater(() -> {
            int rowCount = quoteTable.getTableModel().getRowCount();
            for (int i = 0; i < rowCount; i++) {
                if (quoteTable.getTableModel().getRow(i).get(0).equals(symbol)) {
                    quoteTable.getTableModel().removeRow(i);
                    break;
                }
            }
            quoteTable.getTableModel().addRow(symbol, price, change);
        });
    }

    public void logEvent(String eventMessage) {
        if (gui == null) return;
        gui.getGUIThread().invokeLater(() -> {
            String currentText = eventLog.getText();
            String newText = eventMessage + "\n" + currentText;
            eventLog.setText(newText);
        });
    }
}
